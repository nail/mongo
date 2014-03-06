//@file insert.cpp

/**
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

#include "pch.h"

#include "mongo/db/client.h"
#include "mongo/db/collection.h"
#include "mongo/db/database.h"
#include "mongo/db/oplog.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/jsobjmanipulator.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/structure/catalog/namespace.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    using namespace mongoutils;

    StatusWith<BSONObj> fixDocumentForInsert( const BSONObj& doc ) {
        if ( doc.objsize() > BSONObjMaxUserSize )
            return StatusWith<BSONObj>( ErrorCodes::BadValue,
                                        str::stream()
                                        << "object to insert too large"
                                        << ". size in bytes: " << doc.objsize()
                                        << ", max size: " << BSONObjMaxUserSize );

        bool firstElementIsId = doc.firstElement().fieldNameStringData() == "_id";
        bool hasTimestampToFix = false;
        {
            BSONObjIterator i( doc );
            while ( i.more() ) {
                BSONElement e = i.next();

                if ( e.type() == Timestamp && e.timestampValue() == 0 ) {
                    // we replace Timestamp(0,0) at the top level with a correct value
                    // in the fast pass, we just mark that we want to swap
                    hasTimestampToFix = true;
                    break;
                }

                const char* fieldName = e.fieldName();

    void insertOneObject(Collection *cl, BSONObj &obj, uint64_t flags) {
        validateInsert(obj);
        cl->insertObject(obj, flags);
        cl->notifyOfWriteOp();
    }

    // Does not check magic system collection inserts.
    void _insertObjects(const char *ns, const vector<BSONObj> &objs, bool keepGoing, uint64_t flags, bool logop, bool fromMigrate ) {
        Collection *cl = getOrCreateCollection(ns, logop);
        for (size_t i = 0; i < objs.size(); i++) {
            const BSONObj &obj = objs[i];
            try {
                BSONObj objModified = obj;
                BSONElementManipulator::lookForTimestamps(objModified);
                if (cl->isCapped()) {
                    if (cc().txnStackSize() > 1) {
                        // This is a nightmare to maintain transactionally correct.
                        // Capped collections will be deprecated one day anyway.
                        // They are an anathma.
                        uasserted(17228, "Cannot insert into a capped collection in a multi-statement transaction.");
                    }
                    if (logop) {
                        // special case capped colletions until all oplog writing
                        // for inserts is handled in the collection class, not here.
                        validateInsert(obj);
                        CappedCollection *cappedCl = cl->as<CappedCollection>();
                        bool indexBitChanged = false; // need to initialize this
                        cappedCl->insertObjectAndLogOps(objModified, flags, &indexBitChanged);
                        // Hack copied from Collection::insertObject. TODO: find a better way to do this                        
                        if (indexBitChanged) {
                            cl->noteMultiKeyChanged();
                        }
                        cl->notifyOfWriteOp();
                    }
                    if ( e.type() == Array ) {
                        return StatusWith<BSONObj>( ErrorCodes::BadValue,
                                                    "can't use an array for _id" );
                    }
                    if ( e.type() == Object ) {
                        BSONObj o = e.Obj();
                        Status s = o.storageValidEmbedded();
                        if ( !s.isOK() )
                            return StatusWith<BSONObj>( s );
                    }
                }

            }
        }

        if ( firstElementIsId && !hasTimestampToFix )
            return StatusWith<BSONObj>( BSONObj() );

        bool hadId = firstElementIsId;

        BSONObjIterator i( doc );

        BSONObjBuilder b( doc.objsize() + 16 );
        if ( firstElementIsId ) {
            b.append( doc.firstElement() );
            i.next();
        }
        else {
            BSONElement e = doc["_id"];
            if ( e.type() ) {
                b.append( e );
                hadId = true;
            }
            else {
                b.appendOID( "_id", NULL, true );
            }
        }
    }

    static BSONObj stripDropDups(const BSONObj &obj) {
        BSONObjBuilder b;
        for (BSONObjIterator it(obj); it.more(); ) {
            BSONElement e = it.next();
            if (StringData(e.fieldName()) == "dropDups") {
                warning() << "dropDups is not supported because it deletes arbitrary data." << endl;
                warning() << "We'll proceed without it but if there are duplicates, the index build will fail." << endl;
            } else {
                b.append(e);
            }
            else {
                b.append( e );
            }
        }
        return b.obj();
    }

    Status userAllowedWriteNS( const NamespaceString& ns ) {
        return userAllowedWriteNS( ns.db(), ns.coll() );
    }

    Status userAllowedWriteNS( const StringData& db, const StringData& coll ) {
        // validity checking

        if ( db.size() == 0 )
            return Status( ErrorCodes::BadValue, "db cannot be blank" );

        if ( !NamespaceString::validDBName( db ) )
            return Status( ErrorCodes::BadValue, "invalid db name" );

        if ( coll.size() == 0 )
            return Status( ErrorCodes::BadValue, "collection cannot be blank" );

        if ( !NamespaceString::validCollectionName( coll ) )
            return Status( ErrorCodes::BadValue, "invalid collection name" );

        if ( db.size() + 1 /* dot */ + coll.size() > Namespace::MaxNsColletionLen )
            return Status( ErrorCodes::BadValue,
                           str::stream()
                             << "fully qualified namespace " << db << '.' << coll << " is too long "
                             << "(max is " << Namespace::MaxNsColletionLen << " bytes)" );

        // check spceial areas

        if ( db == "system" )
            return Status( ErrorCodes::BadValue, "cannot use 'system' database" );


        if ( coll.startsWith( "system." ) ) {
            if ( coll == "system.indexes" ) return Status::OK();
            if ( coll == "system.js" ) return Status::OK();
            if ( coll == "system.profile" ) return Status::OK();
            if ( coll == "system.users" ) return Status::OK();
            if ( db == "admin" ) {
                if ( coll == "system.version" ) return Status::OK();
                if ( coll == "system.roles" ) return Status::OK();
                if ( coll == "system.new_users" ) return Status::OK();
                if ( coll == "system.backup_users" ) return Status::OK();
            }
        }
        _insertObjects(ns, objs, keepGoing, flags, logop, fromMigrate);
    }

    void insertObject(const char *ns, const BSONObj &obj, uint64_t flags, bool logop, bool fromMigrate) {
        vector<BSONObj> objs(1);
        objs[0] = obj;
        insertObjects(ns, objs, false, flags, logop, fromMigrate);
    }

} // namespace mongo
