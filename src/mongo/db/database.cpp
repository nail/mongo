// database.cpp

/**
*    Copyright (C) 2008 10gen Inc.
*    Copyright (C) 2013 Tokutek Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "mongo/pch.h"

#include "mongo/db/clientcursor.h"
#include "mongo/db/database.h"
#include "mongo/db/databaseholder.h"
#include "mongo/db/instance.h"
#include "mongo/db/introspect.h"
#include "mongo/db/namespacestring.h"

namespace mongo {


    Database::~Database() {
        verify( Lock::isW() );
        _magic = 0;
        if( _ccByLoc.size() ) {
            log() << "\n\n\nWARNING: ccByLoc not empty on database close! "
                  << _ccByLoc.size() << ' ' << _name << endl;
        }

        for ( CollectionMap::iterator i = _collections.begin(); i != _collections.end(); ++i ) {
            delete i->second;
        }
        _collections.clear();
    }

    Database::Database(const StringData &name, const StringData &path)
        : _name(name.toString()), _path(path.toString()), _collectionMap( _path, _name ),
          _profileName(getSisterNS(_name, "system.profile"))
    {
        try {
            // check db name is valid
            size_t L = name.size();
            uassert( 10028 ,  "db name is empty", L > 0 );
            uassert( 10032 ,  "db name too long", L < 64 );
            uassert( 10029 ,  "bad db name [1]", name[0] != '.' );
            uassert( 10030 ,  "bad db name [2]", name[L-1] != '.' );
            uassert( 10031 ,  "bad char(s) in db name", name.find(' ') == string::npos );
#ifdef _WIN32
            static const char* windowsReservedNames[] = {
                "con", "prn", "aux", "nul",
                "com1", "com2", "com3", "com4", "com5", "com6", "com7", "com8", "com9",
                "lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9"
            };
            for ( size_t i = 0; i < (sizeof(windowsReservedNames) / sizeof(char*)); ++i ) {
                if ( strcasecmp( name, windowsReservedNames[i] ) == 0 ) {
                    stringstream errorString;
                    errorString << "db name \"" << name << "\" is a reserved name";
                    uassert( 16185 , errorString.str(), false );
                }
            }
#endif
            _profile = cmdLine.defaultProfile;
            // The underlying dbname.ns dictionary is opend if it exists,
            // and created lazily on the next write.
            _collectionMap.init();
        } catch (std::exception &e) {
            log() << "warning database " << _path << " " << _name << " could not be opened" << endl;
            DBException* dbe = dynamic_cast<DBException*>(&e);
            if ( dbe != 0 ) {
                log() << "DBException " << dbe->getCode() << ": " << e.what() << endl;
            }
            else {
                log() << e.what() << endl;
            }
            throw;
        }
    }

    void Database::diskSize(size_t &uncompressedSize, size_t &compressedSize) {
        list<string> colls;
        _collectionMap.getNamespaces(colls);
        CollectionData::Stats dbstats;
        for (list<string>::const_iterator it = colls.begin(); it != colls.end(); ++it) {
            Collection *c = getCollection(*it);
            if (c == NULL) {
                DEV warning() << "collection " << *it << " wasn't found in Database::diskSize" << endl;
                continue;
            }
            c->fillCollectionStats(dbstats, NULL, 1);
        }
        uncompressedSize += dbstats.size + dbstats.indexSize;
        compressedSize += dbstats.storageSize + dbstats.indexStorageSize;
    }

    bool Database::setProfilingLevel( int newLevel , string& errmsg ) {
        if ( _profile == newLevel )
            return true;

        if ( newLevel < 0 || newLevel > 2 ) {
            errmsg = "profiling level has to be >=0 and <= 2";
            return false;
        }

        if ( newLevel == 0 ) {
            _profile = 0;
            return true;
        }

        verify( cc().database() == this );

        if (!getOrCreateProfileCollection(this, true))
            return false;

        _profile = newLevel;
        return true;
    }

    Status Database::dropCollection( const StringData& fullns ) {
        LOG(1) << "dropCollection: " << fullns << endl;

        Collection* collection = getCollection( fullns );
        if ( !collection ) {
            // collection doesn't exist
            return Status::OK();
        }

        _initForWrites();

        {
            NamespaceString s( fullns );
            verify( s.db() == _name );

            if( s.isSystem() ) {
                if( s.coll() == "system.profile" ) {
                    if ( _profile != 0 )
                        return Status( ErrorCodes::IllegalOperation,
                                       "turn off profiling before dropping system.profile collection" );
                }
                else {
                    return Status( ErrorCodes::IllegalOperation, "can't drop system ns" );
                }
            }
        }

        BackgroundOperation::assertNoBgOpInProgForNs( fullns );

        if ( collection->_details->getTotalIndexCount() > 0 ) {
            try {
                string errmsg;
                BSONObjBuilder result;

                if ( !dropIndexes( collection->_details, fullns, "*", errmsg, result, true) ) {
                    warning() << "could not drop collection: " << fullns
                              << " because of " << errmsg << endl;
                    return Status( ErrorCodes::InternalError, errmsg );
                }
            }
            catch( DBException& e ) {
                stringstream ss;
                ss << "drop: dropIndexes for collection failed - consider trying repair ";
                ss << " cause: " << e.what();
                warning() << ss.str() << endl;
                return Status( ErrorCodes::InternalError, ss.str() );
            }
            verify( collection->_details->getTotalIndexCount() == 0 );
        }
        LOG(1) << "\t dropIndexes done" << endl;

        ClientCursor::invalidate( fullns );
        Top::global.collectionDropped( fullns );

        Status s = _dropNS( fullns );

        _clearCollectionCache( fullns ); // we want to do this always

        if ( !s.isOK() )
            return s;

        DEV {
            // check all index collection entries are gone
            string nstocheck = fullns.toString() + ".$";
            scoped_lock lk( _collectionLock );
            for ( CollectionMap::iterator i = _collections.begin();
                  i != _collections.end();
                  ++i ) {
                string temp = i->first;
                if ( temp.find( nstocheck ) != 0 )
                    continue;
                log() << "after drop, bad cache entries for: "
                      << fullns << " have " << temp;
                verify(0);
            }
        }

        return Status::OK();
    }

    void Database::_clearCollectionCache( const StringData& fullns ) {
        scoped_lock lk( _collectionLock );
        _clearCollectionCache_inlock( fullns );
    }

    void Database::_clearCollectionCache_inlock( const StringData& fullns ) {
        verify( _name == nsToDatabaseSubstring( fullns ) );
        CollectionMap::iterator it = _collections.find( fullns.toString() );
        if ( it == _collections.end() )
            return;

        delete it->second;
        _collections.erase( it );
    }

    Collection* Database::getCollection( const StringData& ns ) {
        verify( _name == nsToDatabaseSubstring( ns ) );

        scoped_lock lk( _collectionLock );

        string myns = ns.toString();

        CollectionMap::const_iterator it = _collections.find( myns );
        if ( it != _collections.end() ) {
            if ( it->second ) {
                DEV {
                    NamespaceDetails* details = _namespaceIndex.details( ns );
                    if ( details != it->second->_details ) {
                        log() << "about to crash for mismatch on ns: " << ns
                              << " current: " << (void*)details
                              << " cached: " << (void*)it->second->_details;
                    }
                    verify( details == it->second->_details );
                }
                return it->second;
            }
        }

        NamespaceDetails* details = _namespaceIndex.details( ns );
        if ( !details ) {
            return NULL;
        }

        Collection* c = new Collection( ns, details, this );
        _collections[myns] = c;
        return c;
    }



    Status Database::renameCollection( const StringData& fromNS, const StringData& toNS,
                                       bool stayTemp ) {

        // move data namespace
        Status s = _renameSingleNamespace( fromNS, toNS, stayTemp );
        if ( !s.isOK() )
            return s;

        NamespaceDetails* details = _namespaceIndex.details( toNS );
        verify( details );

        // move index namespaces
        string indexName = _name + ".system.indexes";
        BSONObj oldIndexSpec;
        while( Helpers::findOne( indexName, BSON( "ns" << fromNS ), oldIndexSpec ) ) {
            oldIndexSpec = oldIndexSpec.getOwned();

        Client::Context * ctx = cc().getContext();
        verify( ctx );
        verify( ctx->inDB( name , path ) );
        Database *database = ctx->db();
        verify( database->name() == name );

        /* important: kill all open cursors on the database */
        string prefix(name.toString() + ".");
        ClientCursor::invalidate(prefix);

        dbHolderW().erase( name, path );
        ctx->_clear();
        delete database; // closes files
    }

    void DatabaseHolder::closeDatabases(const StringData &path) {
        Paths::const_iterator pi = _paths.find(path);
        if (pi != _paths.end()) {
            const DBs &dbs = pi->second;
            while (!dbs.empty()) {
                DBs::const_iterator it = dbs.begin();
                Database *db = it->second;
                dassert(db->name() == it->first);
                // This erases dbs[db->name] for us, can't lift it out yet until we understand the callers of closeDatabase().
                // That's why we have a weird loop here.
                LOCK_REASON(lockReason, "closing databases");
                Client::WriteContext ctx(db->name(), lockReason);
                db->closeDatabase(db->name(), path);
            }
            _paths.erase(path);
        }
    }

    Database* DatabaseHolder::getOrCreate( const StringData &ns , const StringData& path ) {
        Lock::assertAtLeastReadLocked(ns);
        Database *db;

        // Try first holding a shared lock
        {
            scoped_lock lk( _collectionLock );
            _clearCollectionCache_inlock( fromNSString );
            _clearCollectionCache_inlock( toNSString );
        }

        ClientCursor::invalidate( fromNSString.c_str() );
        ClientCursor::invalidate( toNSString.c_str() );

        // at this point, we haven't done anything destructive yet

        // ----
        // actually start moving
        // ----

        // this could throw, but if it does we're ok
        _namespaceIndex.add_ns( toNS, fromDetails );
        NamespaceDetails* toDetails = _namespaceIndex.details( toNS );

        try {
            toDetails->copyingFrom(toNSString.c_str(), fromDetails); // fixes extraOffset
        }
        catch( DBException& ) {
            // could end up here if .ns is full - if so try to clean up / roll back a little
            _namespaceIndex.kill_ns( toNSString );
            _clearCollectionCache(toNSString);
            throw;
        }

        // If we didn't find it, take an exclusive lock and check
        // again. If it's still not there, do the open.
        {
            SimpleRWLock::Exclusive lk(_rwlock);
            db = _get(ns, path);
            if (db == NULL) {
                StringData dbname = _todb( ns );
                DBs &m = _paths[path];
                if( logLevel >= 1 || m.size() > 40 || DEBUG_BUILD ) {
                    log() << "opening db: " << (path==dbpath?"":path) << ' ' << dbname << endl;
                }

                db = new Database( dbname , path );

                verify( m[dbname] == 0 );
                m[dbname] = db;
                _size++;
            }
        }

        return db;
    }

    void dropDatabase(const StringData& name) {
        TOKULOG(1) << "dropDatabase " << name << endl;
        Lock::assertWriteLocked(name);
        Database *d = cc().database();
        verify(d != NULL);
        verify(d->name() == name);

        // Disable dropDatabase in a multi-statement transaction until
        // we have the time/patience to test/debug it.
        if (cc().txnStackSize() > 1) {
            uasserted(16777, "Cannot dropDatabase in a multi-statement transaction.");
        }

        collectionMap(name)->drop();
        Database::closeDatabase(d->name().c_str(), d->path());
    }

} // namespace mongo
