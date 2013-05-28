//@file update.cpp

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

#include "pch.h"

#include "mongo/db/ops/update.h"

#include "mongo/client/dbclientinterface.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/namespace_details.h"
#include "mongo/db/ops/update_driver.h"
#include "mongo/db/ops/update_internal.h"
#include "mongo/db/pagefault.h"
#include "mongo/db/pdfile.h"
#include "mongo/db/query_optimizer.h"
#include "mongo/db/query_runner.h"
#include "mongo/db/queryutil.h"
#include "mongo/db/record.h"
#include "mongo/db/repl/oplog.h"
#include "mongo/db/server_parameters.h"

//#define DEBUGUPDATE(x) cout << x << endl;
#define DEBUGUPDATE(x)

namespace mongo {

    MONGO_EXPORT_SERVER_PARAMETER( newUpdateFrameworkEnabled, bool, false );

    bool isNewUpdateFrameworkEnabled() {
        return false;
    }

    void checkNoMods( BSONObj o ) {
        BSONObjIterator i( o );
        while( i.moreWithEOO() ) {
            BSONElement e = i.next();
            if ( e.eoo() )
                break;
            uassert( 10154 ,  "Modifiers and non-modifiers cannot be mixed", e.fieldName()[ 0 ] != '$' );
        }
        // no clustering secondary keys
        return false;
    }

    void updateOneObject(Collection *cl, const BSONObj &pk, 
                         const BSONObj &oldObj, BSONObj &newObj, 
                         const BSONObj &updateobj,
                         const bool fromMigrate,
                         uint64_t flags) {
        if (flags & Collection::KEYS_UNAFFECTED_HINT && !updateobj.isEmpty() && !hasClusteringSecondaryKey(cl)) {
            // - operator style update gets applied as an update message
            // - does not maintain sencondary indexes so we can only do it
            // when no indexes were affected
            cl->updateObjectMods(pk, updateobj, fromMigrate, flags);
        } else {
            cl->updateObject(pk, oldObj, newObj, fromMigrate, flags);
        }
        cl->notifyOfWriteOp();
    }

    static void checkNoMods(const BSONObj &obj) {
        for (BSONObjIterator i(obj); i.more(); ) {
            const BSONElement &e = i.next();
            uassert(10154, "Modifiers and non-modifiers cannot be mixed", e.fieldName()[0] != '$');
        }
    }

    static void checkTooLarge(const BSONObj &obj) {
        uassert(12522, "$ operator made object too large", obj.objsize() <= BSONObjMaxUserSize);
    }

    ExportedServerParameter<bool> _fastupdatesParameter(
            ServerParameterSet::getGlobal(), "fastupdates", &cmdLine.fastupdates, true, true);
    ExportedServerParameter<bool> _fastupdatesIgnoreErrorsParameter(
            ServerParameterSet::getGlobal(), "fastupdatesIgnoreErrors", &cmdLine.fastupdatesIgnoreErrors, true, true);

    static Counter64 fastupdatesErrors;
    static ServerStatusMetricField<Counter64> fastupdatesIgnoredErrorsDisplay("fastupdates.errors", &fastupdatesErrors);

    // Apply an update message supplied by a collection to
    // some row in an in IndexDetails (for fast ydb updates).
    //
    class ApplyUpdateMessage : public storage::UpdateCallback {
        // @param pkQuery - the pk with field names, for proper default obj construction
        //                  in mods.createNewFromQuery().
        BSONObj applyMods(const BSONObj &oldObj, const BSONObj &msg) {
            try {
                // The update message is simply an update object, supplied by the user.
                ModSet mods(msg);
                auto_ptr<ModSetState> mss = mods.prepare(oldObj, false);
                const BSONObj newObj = mss->createNewFromMods();
                checkTooLarge(newObj);
                return newObj;
            } catch (const std::exception &ex) {
                // Applying an update message in this fashion _always_ ignores errors.
                // That is the risk you take when using --fastupdates.
                //
                // We will print such errors to the server's error log no more than once per 5 seconds.
                if (!cmdLine.fastupdatesIgnoreErrors && _loggingTimer.millisReset() > 5000) {
                    problem() << "* Failed to apply \"--fastupdate\" updateobj message! "
                                 "This means an update operation that appeared successful actually failed." << endl;
                    problem() << "* It probably should not be happening in production. To ignore these errors, "
                                 "set the server parameter fastupdatesIgnoreErrors=true" << endl;
                    problem() << "*    doc: " << oldObj << endl;
                    problem() << "*    updateobj: " << msg << endl;
                    problem() << "*    exception: " << ex.what() << endl;
                }
                fastupdatesErrors.increment(1);
                return oldObj;
            }
        }
    private:
        Timer _loggingTimer;
    } _storageUpdateCallback; // installed as the ydb update callback in db.cpp via set_update_callback

    static void updateUsingMods(const char *ns, Collection *cl, const BSONObj &pk, const BSONObj &obj,
                                const BSONObj &updateobj, shared_ptr<ModSet> mods, MatchDetails* details,
                                const bool fromMigrate) {
        ModSet *useMods = mods.get();
        auto_ptr<ModSet> mymodset;
        bool hasDynamicArray = mods->hasDynamicArray();
        if (details->hasElemMatchKey() && hasDynamicArray) {
            useMods = mods->fixDynamicArray(details->elemMatchKey());
            mymodset.reset(useMods);
        }
        auto_ptr<ModSetState> mss = useMods->prepare(obj, false /* not an insertion */);
        BSONObj newObj = mss->createNewFromMods();
        checkTooLarge(newObj);
        bool modsAreIndexed = useMods->isIndexed() > 0;
        bool forceFullUpdate = hasDynamicArray || !cl->updateObjectModsOk();
        // adding cl->indexBuildInProgress() as a check below due to #1085
        // This is a little heavyweight, as we whould be able to have modsAreIndexed
        // take hot indexes into account. Unfortunately, that code right now is not
        // factored cleanly enough to do nicely, so we just do the heavyweight check
        // here. Hope to get this properly fixed soon.
        uint64_t flags = (modsAreIndexed || cl->indexBuildInProgress()) ? 
            0 : 
            Collection::KEYS_UNAFFECTED_HINT;
        updateOneObject(cl, pk, obj, newObj,
            forceFullUpdate ? BSONObj() : updateobj, // if we have a dynamic array, force it to do a full overwrite
            fromMigrate, flags);

        // must happen after updateOneObject
        if (forceFullUpdate) {
            OplogHelpers::logUpdate(ns, pk, obj, newObj, fromMigrate);
        }
        else {
            OplogHelpers::logUpdateModsWithRow(ns, pk, obj, updateobj, fromMigrate);
        }
    }

    static void updateNoMods(const char *ns, Collection *cl, const BSONObj &pk, const BSONObj &obj, BSONObj &updateobj,
                             const bool fromMigrate) {
        // This is incredibly un-intiutive, but it takes a const BSONObj
        // and modifies it in-place if a timestamp needs to be set.
        BSONElementManipulator::lookForTimestamps(updateobj);
        checkNoMods(updateobj);
        updateOneObject(cl, pk, obj, updateobj, BSONObj(), fromMigrate, 0);
        // must happen after updateOneObject
        OplogHelpers::logUpdate(ns, pk, obj, updateobj, fromMigrate);
    }

    static UpdateResult upsertAndLog(Collection *cl, const BSONObj &patternOrig,
                                     const BSONObj &updateobj, const bool isOperatorUpdate,
                                     ModSet *mods, bool fromMigrate) {
        const string &ns = cl->ns();
        uassert(16893, str::stream() << "Cannot upsert a collection under-going bulk load: " << ns,
                       ns != cc().bulkLoadNS());

        BSONObj newObj = updateobj;
        if (isOperatorUpdate) {
            newObj = mods->createNewFromQuery(patternOrig);
            cc().curop()->debug().fastmodinsert = true;
        } else {
            cc().curop()->debug().upsert = true;
        }

        checkNoMods(newObj);
        insertOneObject(cl, newObj);
        OplogHelpers::logInsert(ns.c_str(), newObj, fromMigrate);
        return UpdateResult(0, isOperatorUpdate, 1, newObj);
    }

    static UpdateResult updateByPK(const char *ns, Collection *cl,
                            const BSONObj &pk, const BSONObj &patternOrig,
                            const BSONObj &updateobj,
                            const bool upsert,
                            const bool fromMigrate,
                            uint64_t flags) {
        // Create a mod set for $ style updates.
        shared_ptr<ModSet> mods;
        const bool isOperatorUpdate = updateobj.firstElementFieldName()[0] == '$';
        if (isOperatorUpdate) {
            mods.reset(new ModSet(updateobj, cl->indexKeys()));
        }

        BSONObj obj;
        ResultDetails queryResult;
        if (mods && mods->hasDynamicArray()) {
            queryResult.matchDetails.requestElemMatchKey();
        }

        const bool found = queryByPKHack(cl, pk, patternOrig, obj, &queryResult);
        if (!found) {
            if (!upsert) {
                return UpdateResult(0, 0, 0, BSONObj());
            }
            return upsertAndLog(cl, patternOrig, updateobj, isOperatorUpdate, mods.get(), fromMigrate);
        }

        if (isOperatorUpdate) {
            updateUsingMods(ns, cl, pk, obj, updateobj, mods, &queryResult.matchDetails, fromMigrate);
        } else {
            // replace-style update
            BSONObj copy = updateobj.copy();
            updateNoMods(ns, cl, pk, obj, copy, fromMigrate);
        }
        return UpdateResult(1, isOperatorUpdate, 1, BSONObj());
    }

    BSONObj invertUpdateMods(const BSONObj &updateobj) {
        BSONObjBuilder b(updateobj.objsize());
        for (BSONObjIterator i(updateobj); i.more(); ) {
            const BSONElement &e = i.next();
            verify(str::equals(e.fieldName(), "$inc"));
            BSONObjBuilder inc(b.subobjStart("$inc"));
            for (BSONObjIterator o(e.Obj()); o.more(); ) {
                const BSONElement &fieldToInc = o.next();
                verify(fieldToInc.isNumber());
                const long long invertedValue = -fieldToInc.numberLong();
                inc.append(fieldToInc.fieldName(), invertedValue);
            }
            inc.done();
        }
        return b.obj();
    }

    static UpdateResult _updateObjects(const char *ns,
                                       const BSONObj &updateobj,
                                       const BSONObj &patternOrig,
                                       const bool upsert, const bool multi,
                                       const bool fromMigrate) {
        TOKULOG(2) << "update: " << ns
                   << " update: " << updateobj
                   << " query: " << patternOrig
                   << " upsert: " << upsert << " multi: " << multi << endl;

        Collection *cl = getOrCreateCollection(ns, true);

        // Fast-path for simple primary key updates.
        //
        // - We don't do it for capped collections since  their documents may not grow,
        // and the fast path doesn't know if docs grow until the update message is applied.
        // - We don't do it if multi=true because semantically we're not supposed to, if
        // the update ends up being a replace-style upsert. See jstests/update_multi6.js
        if (!multi && !cl->isCapped()) {
            const BSONObj pk = cl->getSimplePKFromQuery(patternOrig);
            if (!pk.isEmpty()) {
                return updateByPK(ns, cl, pk, patternOrig, updateobj,
                                  upsert, fromMigrate, 0);
            }
        }

        // Run a regular update using the query optimizer.

        set<BSONObj> seenObjects;
        MatchDetails details;
        shared_ptr<ModSet> mods;

        const bool isOperatorUpdate = updateobj.firstElementFieldName()[0] == '$';
        if (isOperatorUpdate) {
            mods.reset(new ModSet(updateobj, cl->indexKeys()));
            if (mods->hasDynamicArray()) {
                details.requestElemMatchKey();
            }
        }

        int numModded = 0;
        cc().curop()->debug().nscanned = 0;
        for (shared_ptr<Cursor> c = getOptimizedCursor(ns, patternOrig); c->ok(); ) {
            cc().curop()->debug().nscanned++;
            BSONObj currPK = c->currPK();
            if (c->getsetdup(currPK)) {
                c->advance();
                continue;
            }
            if (!c->currentMatches(&details)) {
                c->advance();
                continue;
            }

            BSONObj currentObj = c->current();
            if (!isOperatorUpdate) {
                // replace-style update only affects a single matching document
                uassert(10158, "multi update only works with $ operators", !multi);
                BSONObj copy = updateobj.copy();
                updateNoMods(ns, cl, currPK, currentObj, copy, fromMigrate);
                return UpdateResult(1, 0, 1, BSONObj());
            }

            // operator-style updates may affect many documents
            if (multi) {
                // Advance past the document to be modified - SERVER-5198,
                // First, get owned copies of currPK/currObj, which live in the cursor.
                currPK = currPK.getOwned();
                currentObj = currentObj.getOwned();
                while (c->ok() && currPK == c->currPK()) {
                    c->advance();
                }

                // Multi updates need to do their own deduplication because updates may modify the
                // keys the cursor is in the process of scanning over.
                if ( seenObjects.count(currPK) ) {
                    continue;
                } else {
                    seenObjects.insert(currPK);
                }
            }

            updateUsingMods(ns, cl, currPK, currentObj, updateobj, mods, &details, fromMigrate);
            numModded++;

            if (!multi) {
                break;
            }
        }

        if (numModded) {
            // We've modified something, so we're done.
            return UpdateResult(1, 1, numModded, BSONObj());
        }
        if (!upsert) {
            // We haven't modified anything, but we're not trying to upsert, so we're done.
            return UpdateResult(0, isOperatorUpdate, numModded, BSONObj());
        }

        if (!isOperatorUpdate) {
            uassert(10159, "multi update only works with $ operators", !multi);
        }
        // Upsert a new object
        return upsertAndLog(cl, patternOrig, updateobj, isOperatorUpdate, mods.get(), fromMigrate);
    }

    UpdateResult _updateObjectsNEW( bool su,
                                    const char* ns,
                                    const BSONObj& updateobj,
                                    const BSONObj& patternOrig,
                                    bool upsert,
                                    bool multi,
                                    bool logop ,
                                    OpDebug& debug,
                                    RemoveSaver* rs,
                                    bool fromMigrate,
                                    const QueryPlanSelectionPolicy& planPolicy,
                                    bool forReplication ) {

        // TODO
        // + Separate UpdateParser from UpdateRunner (the latter should be "stage-y")
        // + fast path for update for query by _id
        // + $atomic support (or better, support proper yielding if not)
        // + define the dedup story (and do it here, if that's the decidion)
        // + support in-place updates (and determination if indices are involved in an update)
        // + update OpDebug counters properly
        // + specific paths set on insert
        // + support for relaxing viable path constraint in replication
        // + set UpdateResponse properly

        UpdateDriver::Options opts;
        opts.multi = multi;
        opts.upsert = upsert;
        opts.logOp = logop;
        UpdateDriver driver(opts);
        Status status = driver.parse( updateobj );
        if ( !status.isOK() ) {
            uasserted( 16840, status.reason() );
        }

        shared_ptr<Cursor> cursor = getOptimizedCursor( ns, patternOrig, BSONObj(), planPolicy );
        NamespaceDetails* d = nsdetails(ns); // can be null if an upsert...
        NamespaceDetailsTransient* nsdt = &NamespaceDetailsTransient::get(ns);

        // We may or may not have documents for this update. If we don't, then try to upsert,
        // if allowed.
        if ( !cursor->ok() && upsert ) {

            // If this is not a full object replace, we need to generate a document by
            // examining the query. Otherwise, we can use the replacement object itself.
            BSONObj oldObj;
            if ( *updateobj.firstElementFieldName() == '$' ) {
                if ( !driver.createFromQuery( patternOrig, &oldObj ) ) {
                    uasserted( 16835, "cannot create object to update" );
                }
                // TODO this is the hook for activating a $setOnInsert
            }

            // Since this is an upsert, we will be oplogging it as an insert. We don't
            // need the driver's help to build the oplog record, then.
            driver.setLogOp(false);

            BSONObj newObj;
            status = driver.update( oldObj, StringData(), &newObj, NULL /* no oplog record */);
            if ( !status.isOK() ) {
                uasserted( 16836, status.reason() );
            }

            theDataFileMgr.insertWithObjMod( ns, newObj, false, su );

            if ( logop ) {
                logOp( "i", ns, newObj, 0, 0, fromMigrate, &newObj );
            }

            return UpdateResult( false, false, 0, BSONObj() );
        }

        // We have documents for this update. Let's fetch each of them and pipe them through
        // the update expression.
        while ( cursor->ok() ) {

            // Get Obj and match details
            Record* r = cursor->_current();
            DiskLoc loc = cursor->currLoc();
            const BSONObj oldObj = loc.obj();


            // Skip documents that don't match. Also, we don't want to update the very object
            // atop of which the cursor is, becuase that document may move. So we advance the
            // cursor before operating on the record.
            MatchDetails matchDetails;
            matchDetails.requestElemMatchKey();
            if ( !cursor->currentMatches( &matchDetails ) ) {
                cursor->advance();
                continue;
            }
            else if (multi) {
                cursor->advance();
            }

            BSONObj newObj;
            BSONObj logObj;
            status = driver.update( oldObj, matchDetails.elemMatchKey(), &newObj, &logObj );
            if ( !status.isOK() ) {
                uasserted( 16837, status.reason() );
            }

            // Write Obj
            theDataFileMgr.updateRecord(ns,
                                        d,
                                        nsdt,
                                        r,
                                        loc,
                                        newObj.objdata(),
                                        newObj.objsize(),
                                        debug);

            // Log Obj
            if ( logop ) {
                if ( !logObj.isEmpty() ) {
                    BSONObj pattern = patternOrig;
                    logOp("u", ns, logObj , &pattern, 0, fromMigrate, &newObj );
                }
            }

            getDur().commitIfNeeded();

            if (!multi) {
                break;
            }
        }

        return UpdateResult( false, false, 0, BSONObj() );
    }

    UpdateResult updateObjects( const char* ns,
                                const BSONObj& updateobj,
                                const BSONObj& patternOrig,
                                bool upsert,
                                bool multi,
                                bool logop ,
                                OpDebug& debug,
                                bool fromMigrate,
                                const QueryPlanSelectionPolicy& planPolicy ) {

        validateUpdate( ns , updateobj , patternOrig );

        if ( newUpdateFrameworkEnabled ) {

            UpdateResult ur = _updateObjectsNEW(false, ns, updateobj, patternOrig,
                                                upsert, multi, logop,
                                                debug, NULL, fromMigrate, planPolicy );
            debug.nupdated = ur.num;
            return ur;
        }
        else {

            UpdateResult ur = _updateObjects(false, ns, updateobj, patternOrig,
                                             upsert, multi, logop,
                                             debug, NULL, fromMigrate, planPolicy );
            debug.nupdated = ur.num;
            return ur;
        }
    }

    UpdateResult updateObjectsForReplication( const char* ns,
                                              const BSONObj& updateobj,
                                              const BSONObj& patternOrig,
                                              bool upsert,
                                              bool multi,
                                              bool logop ,
                                              OpDebug& debug,
                                              bool fromMigrate,
                                              const QueryPlanSelectionPolicy& planPolicy ) {

        validateUpdate( ns , updateobj , patternOrig );

        if ( newUpdateFrameworkEnabled ) {

            UpdateResult ur = _updateObjectsNEW(false,
                                                ns,
                                                updateobj,
                                                patternOrig,
                                                upsert,
                                                multi,
                                                logop,
                                                debug,
                                                NULL /* no remove saver */,
                                                fromMigrate,
                                                planPolicy,
                                                true /* for replication */ );
            debug.nupdated = ur.num;
            return ur;

        }
        else {

            UpdateResult ur = _updateObjects(false,
                                             ns,
                                             updateobj,
                                             patternOrig,
                                             upsert,
                                             multi,
                                             logop,
                                             debug,
                                             NULL /* no remove saver */,
                                             fromMigrate,
                                             planPolicy,
                                             true /* for replication */ );
            debug.nupdated = ur.num;
            return ur;

        }
    }

    BSONObj applyUpdateOperators( const BSONObj& from, const BSONObj& operators ) {
        if ( newUpdateFrameworkEnabled ) {
            UpdateDriver::Options opts;
            opts.multi = false;
            opts.upsert = false;
            UpdateDriver driver(opts);
            Status status = driver.parse( operators );
            if ( !status.isOK() ) {
                uasserted( 16838, status.reason() );
            }

            BSONObj newObj;
            status = driver.update( from, StringData(), &newObj, NULL /* not oplogging */ );
            if ( !status.isOK() ) {
                uasserted( 16839, status.reason() );
            }

            return newObj;
        }
        else {
            ModSet mods( operators );
            return mods.prepare( from, false /* not an insertion */ )->createNewFromMods();
        }
    }

}  // namespace mongo
