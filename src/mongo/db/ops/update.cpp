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

#include <cstring>  // for memcpy

#include "mongo/bson/mutable/algorithm.h"
#include "mongo/bson/mutable/damage_vector.h"
#include "mongo/bson/mutable/document.h"
#include "mongo/client/dbclientinterface.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/index_set.h"
#include "mongo/db/namespace_details.h"
#include "mongo/db/ops/update_driver.h"
#include "mongo/db/ops/update_executor.h"
#include "mongo/db/ops/update_lifecycle.h"
#include "mongo/db/pagefault.h"
#include "mongo/db/pdfile.h"
#include "mongo/db/query_optimizer.h"
#include "mongo/db/query_runner.h"
#include "mongo/db/query/new_find.h"
#include "mongo/db/query/query_planner_common.h"
#include "mongo/db/query/runner_yield_policy.h"
#include "mongo/db/queryutil.h"
#include "mongo/db/repl/is_master.h"
#include "mongo/db/repl/oplog.h"
#include "mongo/db/storage/record.h"
#include "mongo/db/structure/collection.h"
#include "mongo/platform/unordered_set.h"

namespace mongo {

    namespace mb = mutablebson;
    namespace {

        const char idFieldName[] = "_id";
        const FieldRef idFieldRef(idFieldName);

        // TODO: Make this a function on NamespaceString, or make it cleaner.
        inline void validateUpdate(const char* ns ,
                                   const BSONObj& updateobj,
                                   const BSONObj& patternOrig) {
            uassert(10155 , "cannot update reserved $ collection", strchr(ns, '$') == 0);
            if (strstr(ns, ".system.")) {
                /* dm: it's very important that system.indexes is never updated as IndexDetails
                   has pointers into it */
                uassert(10156,
                         str::stream() << "cannot update system collection: "
                         << ns << " q: " << patternOrig << " u: " << updateobj,
                         legalClientSystemNS(ns , true));
            }
        }

        Status storageValid(const mb::Document&, const bool);
        Status storageValid(const mb::ConstElement&, const bool);
        Status storageValidChildren(const mb::ConstElement&, const bool);

        /**
         * mutable::document storageValid check -- like BSONObj::_okForStorage
         */
        Status storageValid(const mb::Document& doc, const bool deep = true) {
            mb::ConstElement currElem = doc.root().leftChild();
            while (currElem.ok()) {
                if (currElem.getFieldName() == idFieldName) {
                    switch (currElem.getType()) {
                        case RegEx:
                        case Array:
                        case Undefined:
                            return Status(ErrorCodes::InvalidIdField,
                                          str::stream() << "The '_id' value cannot be of type "
                                                        << typeName(currElem.getType()));
                        default:
                            break;
                    }
                }
                Status s = storageValid(currElem, deep);
                if (!s.isOK())
                    return s;
                currElem = currElem.rightSibling();
            }

            return Status::OK();
        }

        Status storageValid(const mb::ConstElement& elem, const bool deep = true) {
            if (!elem.ok())
                return Status(ErrorCodes::BadValue, "Invalid elements cannot be stored.");

            StringData fieldName = elem.getFieldName();
            // Cannot start with "$", unless dbref which must start with ($ref, $id)
            if (fieldName[0] == '$') {
                // Check if it is a DBRef has this field {$ref, $id, [$db]}
                mb::ConstElement curr = elem;
                StringData currName = fieldName;

                // Found a $db field
                if (currName == "$db") {
                    if (curr.getType() != String) {
                        return Status(ErrorCodes::InvalidDBRef,
                                str::stream() << "The DBRef $db field must be a String, not a "
                                              << typeName(curr.getType()));
                    }
                    curr = curr.leftSibling();

                    if (!curr.ok() || (curr.getFieldName() != "$id"))
                        return Status(ErrorCodes::InvalidDBRef,
                                      "Found $db field without a $id before it, which is invalid.");

                    currName = curr.getFieldName();
                }

                // Found a $id field
                if (currName == "$id") {
                    Status s = storageValidChildren(curr, deep);
                    if (!s.isOK())
                        return s;

                    curr = curr.leftSibling();
                    if (!curr.ok() || (curr.getFieldName() != "$ref")) {
                        return Status(ErrorCodes::InvalidDBRef,
                                     "Found $id field without a $ref before it, which is invalid.");
                    }

                    currName = curr.getFieldName();
                }

                if (currName == "$ref") {
                    if (curr.getType() != String) {
                        return Status(ErrorCodes::InvalidDBRef,
                                str::stream() << "The DBRef $ref field must be a String, not a "
                                              << typeName(curr.getType()));
                    }

                    if (!curr.rightSibling().ok() || curr.rightSibling().getFieldName() != "$id")
                        return Status(ErrorCodes::InvalidDBRef,
                                str::stream() << "The DBRef $ref field must be "
                                                 "following by a $id field");
                }
                else {
                    // not an okay, $ prefixed field name.
                    return Status(ErrorCodes::DollarPrefixedFieldName,
                                  str::stream() << elem.getFieldName()
                                                << " is not valid for storage.");
                }
            }

            // Field name cannot have a "." in it.
            if (fieldName.find(".") != string::npos) {
                return Status(ErrorCodes::DottedFieldName,
                              str::stream() << elem.getFieldName() << " is not valid for storage.");
            }

            // Check children if there are any.
            Status s = storageValidChildren(elem, deep);
            if (!s.isOK())
                return s;

            return Status::OK();
        }

        Status storageValidChildren(const mb::ConstElement& elem, const bool deep) {
            if (!elem.hasChildren())
                return Status::OK();

            mb::ConstElement curr = elem.leftChild();
            while (curr.ok()) {
                Status s = storageValid(curr, deep);
                if (!s.isOK())
                    return s;
                curr = curr.rightSibling();
            }

            return Status::OK();
        }

        /**
         * This will verify that all updated fields are
         *   1.) Valid for storage (checking parent to make sure things like DBRefs are valid)
         *   2.) Compare updated immutable fields do not change values
         *
         * If updateFields is empty then it was replacement and/or we need to check all fields
         */
        inline Status validate(const BSONObj& original,
                               const FieldRefSet& updatedFields,
                               const mb::Document& updated,
                               const std::vector<FieldRef*>* immutableAndSingleValueFields,
                               const ModifierInterface::Options& opts) {

            LOG(3) << "update validate options -- "
                   << " updatedFields: " << updatedFields
                   << " immutableAndSingleValueFields.size:"
                   << (immutableAndSingleValueFields ? immutableAndSingleValueFields->size() : 0)
                   << " fromRepl: " << opts.fromReplication
                   << " validate:" << opts.enforceOkForStorage;

            // 1.) Loop through each updated field and validate for storage
            // and detect immutable field updates

            // The set of possibly changed immutable fields -- we will need to check their vals
            FieldRefSet changedImmutableFields;

            // Check to see if there were no fields specified or if we are not validating
            // The case if a range query, or query that didn't result in saved fields
            if (updatedFields.empty() || !opts.enforceOkForStorage) {
                if (opts.enforceOkForStorage) {
                    // No specific fields were updated so the whole doc must be checked
                    Status s = storageValid(updated, true);
                    if (!s.isOK())
                        return s;
                }

                // Check all immutable fields
                if (immutableAndSingleValueFields)
                    changedImmutableFields.fillFrom(*immutableAndSingleValueFields);
            }
            else {

                // TODO: Change impl so we don't need to create a new FieldRefSet
                //       -- move all conflict logic into static function on FieldRefSet?
                FieldRefSet immutableFieldRef;
                if (immutableAndSingleValueFields)
                    immutableFieldRef.fillFrom(*immutableAndSingleValueFields);

                FieldRefSet::const_iterator where = updatedFields.begin();
                const FieldRefSet::const_iterator end = updatedFields.end();
                for( ; where != end; ++where) {
                    const FieldRef& current = **where;

                    // Find the updated field in the updated document.
                    mutablebson::ConstElement newElem = updated.root();
                    size_t currentPart = 0;
                    while (newElem.ok() && currentPart < current.numParts())
                        newElem = newElem[current.getPart(currentPart++)];

                    // newElem might be missing if $unset/$renamed-away
                    if (newElem.ok()) {
                        Status s = storageValid(newElem, true);
                        if (!s.isOK())
                            return s;
                    }
                    // Check if the updated field conflicts with immutable fields
                    immutableFieldRef.findConflicts(&current, &changedImmutableFields);
                }
            }

            const bool idChanged = updatedFields.findConflicts(&idFieldRef, NULL);

            // Add _id to fields to check since it too is immutable
            if (idChanged)
                changedImmutableFields.keepShortest(&idFieldRef);
            else if (changedImmutableFields.empty()) {
                // Return early if nothing changed which is immutable
                return Status::OK();
            }

            LOG(4) << "Changed immutable fields: " << changedImmutableFields;
            // 2.) Now compare values of the changed immutable fields (to make sure they haven't)

            const mutablebson::ConstElement newIdElem = updated.root()[idFieldName];

            FieldRefSet::const_iterator where = changedImmutableFields.begin();
            const FieldRefSet::const_iterator end = changedImmutableFields.end();
            for( ; where != end; ++where ) {
                const FieldRef& current = **where;

                // Find the updated field in the updated document.
                mutablebson::ConstElement newElem = updated.root();
                size_t currentPart = 0;
                while (newElem.ok() && currentPart < current.numParts())
                    newElem = newElem[current.getPart(currentPart++)];

                if (!newElem.ok()) {
                    if (original.isEmpty()) {
                        // If the _id is missing and not required, then skip this check
                        if (!(current.dottedField() == idFieldName))
                            return Status(ErrorCodes::NoSuchKey,
                                          mongoutils::str::stream()
                                          << "After applying the update, the new"
                                          << " document was missing the '"
                                          << current.dottedField()
                                          << "' (required and immutable) field.");

                    }
                    else {
                        if (current.dottedField() != idFieldName)
                            return Status(ErrorCodes::ImmutableField,
                                          mongoutils::str::stream()
                                          << "After applying the update to the document with "
                                          << newIdElem.toString()
                                          << ", the '" << current.dottedField()
                                          << "' (required and immutable) field was "
                                             "found to have been removed --"
                                          << original);
                    }
                }
                else {

                    // Find the potentially affected field in the original document.
                    const BSONElement oldElem = original.getFieldDotted(current.dottedField());
                    const BSONElement oldIdElem = original.getField(idFieldName);

                    // Ensure no arrays since neither _id nor shard keys can be in an array, or one.
                    mb::ConstElement currElem = newElem;
                    while (currElem.ok()) {
                        if (currElem.getType() == Array) {
                            return Status(ErrorCodes::NotSingleValueField,
                                          mongoutils::str::stream()
                                          << "After applying the update to the document {"
                                          << (oldIdElem.ok() ? oldIdElem.toString() :
                                                               newIdElem.toString())
                                          << " , ...}, the (immutable) field '"
                                          << current.dottedField()
                                          << "' was found to be an array or array descendant.");
                        }
                        currElem = currElem.parent();
                    }

                    // If we have both (old and new), compare them. If we just have new we are good
                    if (oldElem.ok() && newElem.compareWithBSONElement(oldElem, false) != 0) {
                        return Status(ErrorCodes::ImmutableField,
                                      mongoutils::str::stream()
                                      << "After applying the update to the document {"
                                      << (oldIdElem.ok() ? oldIdElem.toString() :
                                                           newIdElem.toString())
                                      << " , ...}, the (immutable) field '" << current.dottedField()
                                      << "' was found to have been altered to "
                                      << newElem.toString());
                    }
                }
            }

            return Status::OK();
        }

        Status recoverFromYield(const UpdateRequest& request,
                                UpdateDriver* driver,
                                Collection* collection) {

            const NamespaceString& nsString(request.getNamespaceString());
            // We yielded and recovered OK, and our cursor is still good. Details about
            // our namespace may have changed while we were yielded, so we re-acquire
            // them here. If we can't do so, escape the update loop. Otherwise, refresh
            // the driver so that it knows about what is currently indexed.

            if (request.shouldCallLogOp() && !isMasterNs(nsString.ns().c_str())) {
                return Status(ErrorCodes::NotMaster, mongoutils::str::stream() <<
                              "Demoted from primary while performing update on " << nsString.ns());
            }

            Collection* oldCollection = collection;
            collection = cc().database()->getCollection(nsString.ns());

            // We should not get a new pointer to the same collection...
            if (oldCollection && (oldCollection != collection))
                return Status(ErrorCodes::IllegalOperation,
                              str::stream() << "Collection changed during the Update: ok?"
                                            << " old: " << oldCollection->ok()
                                            << " new:" << collection->ok());

            if (!collection)
                return Status(ErrorCodes::IllegalOperation,
                              "Update aborted due to invalid state transitions after yield -- "
                              "collection pointer NULL.");

            if (!collection->ok())
                return Status(ErrorCodes::IllegalOperation,
                              "Update aborted due to invalid state transitions after yield -- "
                              "collection not ok().");

            IndexCatalog* idxCatalog = collection->getIndexCatalog();
            if (!idxCatalog)
                return Status(ErrorCodes::IllegalOperation,
                              "Update aborted due to invalid state transitions after yield -- "
                              "IndexCatalog pointer NULL.");

            if (!idxCatalog->ok())
                return Status(ErrorCodes::IllegalOperation,
                              "Update aborted due to invalid state transitions after yield -- "
                              "IndexCatalog not ok().");

            if (request.getLifecycle()) {
                UpdateLifecycle* lifecycle = request.getLifecycle();
                lifecycle->setCollection(collection);

                if (!lifecycle->canContinue()) {
                    return Status(ErrorCodes::IllegalOperation,
                                  "Update aborted due to invalid state transitions after yield.",
                                  17270);
                }

                driver->refreshIndexKeys(lifecycle->getIndexKeys());
            }

            return Status::OK();
        }

        Status ensureIdAndFirst(mb::Document& doc) {
            mb::Element idElem = mb::findFirstChildNamed(doc.root(), idFieldName);

            // Move _id as first element if it exists
            if (idElem.ok()) {
                if (idElem.leftSibling().ok()) {
                    Status s = idElem.remove();
                    if (!s.isOK())
                        return s;
                    s = doc.root().pushFront(idElem);
                    if (!s.isOK())
                        return s;
                }
            }
            else {
                // Create _id if the document does not currently have one.
                idElem = doc.makeElementNewOID(idFieldName);
                if (!idElem.ok())
                    return Status(ErrorCodes::BadValue,
                                  "Could not create new _id ObjectId element.",
                                  17268);
                Status s = doc.root().pushFront(idElem);
                if (!s.isOK())
                    return s;
            }

            return Status::OK();

        }
    } // namespace

    UpdateResult update(const UpdateRequest& request, OpDebug* opDebug) {

        UpdateExecutor executor(&request, opDebug);
        return executor.execute();
    }

    UpdateResult update(const UpdateRequest& request, OpDebug* opDebug, UpdateDriver* driver) {

        LOG(3) << "processing update : " << request;
        const NamespaceString& nsString = request.getNamespaceString();
        UpdateLifecycle* lifecycle = request.getLifecycle();
        const CurOp* curOp = cc().curop();
        Collection* collection = cc().database()->getCollection(nsString.ns());

        validateUpdate(nsString.ns().c_str(), request.getUpdates(), request.getQuery());

        const CurOp* curOp = cc().curop();
        Collection* collection = cc().database()->getCollection(nsString.ns());

        // TODO: This seems a bit circuitious.
        opDebug->updateobj = request.getUpdates();

        if (lifecycle) {
            lifecycle->setCollection(collection);
            driver->refreshIndexKeys(lifecycle->getIndexKeys());
        }

        CanonicalQuery* cq;
        if (!CanonicalQuery::canonicalize(nsString, request.getQuery(), &cq).isOK()) {
            uasserted(17242, "could not canonicalize query " + request.getQuery().toString());
        }

        Runner* rawRunner;
        if (!getRunner(collection, cq, &rawRunner).isOK()) {
            uasserted(17243, "could not get runner " + request.getQuery().toString());
        }

        // Create the runner and setup all deps.
        auto_ptr<Runner> runner(rawRunner);

        // Register Runner with ClientCursor
        ClientCursor::registerRunner(runner.get());

        // Cleanup the runner if needed
        const scoped_ptr<DeregisterEvenIfUnderlyingCodeThrows> safety(
                new DeregisterEvenIfUnderlyingCodeThrows(runner.get()));

        // Use automatic yield policy
        runner->setYieldPolicy(Runner::YIELD_AUTO);

        // If the update was marked with '$isolated' (a.k.a '$atomic'), we are not allowed to
        // yield while evaluating the update loop below.
        //
        // TODO: Old code checks this repeatedly within the update loop. Is that necessary? It seems
        // that once atomic should be always atomic.
        const bool isolated =
            cursor->ok() &&
            cursor->matcher() &&
            cursor->matcher()->docMatcher().atomic();

        // The 'cursor' the optimizer gave us may contain query plans that generate duplicate
        // diskloc's. We set up here the mechanims that will prevent us from processing those
        // twice if we see them. We also set up a 'ClientCursor' so that we can support
        // yielding.
        //
        // TODO: Is it valid to call this on a non-ok cursor?
        const bool dedupHere = cursor->autoDedup();

        //
        // We'll start assuming we have one or more documents for this update. (Otherwise,
        // we'll fall-back to insert case (if upsert is true).)
        //

        // We are an update until we fall into the insert case below.
        driver->setContext(ModifierInterface::ExecInfo::UPDATE_CONTEXT);

        // Let's fetch each of them and pipe them through the update expression, making sure to
        // keep track of the necessary stats. Recall that we'll be pulling documents out of
        // cursors and some of them do not deduplicate the entries they generate. We have
        // deduping logic in here, too -- for now.
        unordered_set<DiskLoc, DiskLoc::Hasher> seenLocs;
        int numMatched = 0;

        // NOTE: When doing a multi-update, we only store the locs of moved docs, since the
        // runner will keep track of the rest.
        typedef unordered_set<DiskLoc, DiskLoc::Hasher> DiskLocSet;
        const scoped_ptr<DiskLocSet> updatedLocs(request.isMulti() ? new DiskLocSet : NULL);

        // Reset these counters on each call. We might re-enter this function to retry this
        // update if we throw a page fault exception below, and we rely on these counters
        // reflecting only the actions taken locally. In particlar, we must have the no-op
        // counter reset so that we can meaningfully comapre it with numMatched above.
        opDebug->nscanned = 0;
        opDebug->nscannedObjects = 0;
        opDebug->nModified = 0;

        // Get the cached document from the update driver.
        mutablebson::Document& doc = driver->getDocument();
        mutablebson::DamageVector damages;

        // Used during iteration of docs
        BSONObj oldObj;
        DiskLoc loc;

        // Get first doc, and location
        Runner::RunnerState state = runner->getNext(&oldObj, &loc);
        while (Runner::RUNNER_ADVANCED == state) {

        uassert(ErrorCodes::NotMaster,
                mongoutils::str::stream() << "Not primary while updating " << nsString.ns(),
                !request.shouldCallLogOp() || isMasterNs(nsString.ns().c_str()));

        while (true) {
            // See if we have a write in isolation mode
            isolationModeWriteOccured = isolated && (opDebug->nModified > 0);

            // Change to manual yielding (no yielding) if we have written in isolation mode
            if (isolationModeWriteOccured) {
                runner->setYieldPolicy(Runner::YIELD_MANUAL);
            }

            // keep track of the yield count before calling getNext (which might yield).
            oldYieldCount = curOp->numYields();

            // Get next doc, and location
            DiskLoc loc;
            state = runner->getNext(&oldObj, &loc);
            const bool didYield = (oldYieldCount != curOp->numYields());

            if (state != Runner::RUNNER_ADVANCED) {
                if (state == Runner::RUNNER_EOF) {
                    if (didYield)
                        uassertStatusOK(recoverFromYield(request, driver, collection));

            // Skips this document if it:
            // a) doesn't match the query portion of the update
            // b) was deemed duplicate by the underlying cursor machinery
            //
            // Now, if we are going to update the document,
            // c) we don't want to do so while the cursor is at it, as that may invalidate
            // the cursor. So, we advance to next document, before issuing the update.
            MatchDetails matchDetails;
            matchDetails.requestElemMatchKey();
            if ( !cursor->currentMatches( &matchDetails ) ) {
                // a)
                cursor->advance();
                continue;
            }
            else if ( cursor->getsetdup( loc ) && dedupHere ) {
                // b)
                cursor->advance();
                continue;
            }
            else if (!driver->isDocReplacement() && request.isMulti()) {
                // c)
                cursor->advance();
                if ( dedupHere ) {
                    if ( seenLocs.count( loc ) ) {
                        continue;
                    }
                }

                // There are certain kind of cursors that hold multiple pointers to data
                // underneath. $or cursors is one example. In a $or cursor, it may be the case
                // that when we did the last advance(), we finished consuming documents from
                // one of $or child and started consuming the next one. In that case, it is
                // possible that the last document of the previous child is the same as the
                // first document of the next (see SERVER-5198 and jstests/orp.js).
                //
                // So we advance the cursor here until we see a new diskloc.
                //
                // Note that we won't be yielding, and we may not do so for a while if we find
                // a particularly duplicated sequence of loc's. That is highly unlikely,
                // though.  (See SERVER-5725, if curious, but "stage" based $or will make that
                // ticket moot).
                while( cursor->ok() && loc == cursor->currLoc() ) {
                    cursor->advance();
                }
            }

            // Refresh things after a yield.
            if (didYield)
                uassertStatusOK(recoverFromYield(request, driver, collection));

            // We fill this with the new locs of moved doc so we don't double-update.
            // NOTE: The runner will de-dup non-moved things.
            if (updatedLocs && updatedLocs->count(loc) > 0) {
                continue;
            }

            // We count how many documents we scanned even though we may skip those that are
            // deemed duplicated. The final 'numMatched' and 'nscanned' numbers may differ for
            // that reason.
            // XXX: pull this out of the plan.
            opDebug->nscanned++;

            // Found a matching document
            opDebug->nscannedObjects++;
            numMatched++;

            // Ask the driver to apply the mods. It may be that the driver can apply those "in
            // place", that is, some values of the old document just get adjusted without any
            // change to the binary layout on the bson layer. It may be that a whole new
            // document is needed to accomodate the new bson layout of the resulting document.
            doc.reset(oldObj, mutablebson::Document::kInPlaceEnabled);
            BSONObj logObj;


            FieldRefSet updatedFields;

            Status status = Status::OK();
            if (!driver->needMatchDetails()) {
                // If we don't need match details, avoid doing the rematch
                status = driver->update(StringData(), &doc, &logObj, &updatedFields);
            }
            else {
                // If there was a matched field, obtain it.
                MatchDetails matchDetails;
                matchDetails.requestElemMatchKey();

                dassert(cq);
                verify(cq->root()->matchesBSON(oldObj, &matchDetails));

                string matchedField;
                if (matchDetails.hasElemMatchKey())
                    matchedField = matchDetails.elemMatchKey();

                // TODO: Right now, each mod checks in 'prepare' that if it needs positional
                // data, that a non-empty StringData() was provided. In principle, we could do
                // that check here in an else clause to the above conditional and remove the
                // checks from the mods.

                status = driver->update(matchedField, &doc, &logObj, &updatedFields);
            }

            if (!status.isOK()) {
                uasserted(16837, status.reason());
            }

            // Ensure _id exists and is first
            uassertStatusOK(ensureIdAndFirst(doc));

            // If the driver applied the mods in place, we can ask the mutable for what
            // changed. We call those changes "damages". :) We use the damages to inform the
            // journal what was changed, and then apply them to the original document
            // ourselves. If, however, the driver applied the mods out of place, we ask it to
            // generate a new, modified document for us. In that case, the file manager will
            // take care of the journaling details for us.
            //
            // This code flow is admittedly odd. But, right now, journaling is baked in the file
            // manager. And if we aren't using the file manager, we have to do jounaling
            // ourselves.
            bool objectWasChanged = false;
            BSONObj newObj;
            const char* source = NULL;
            bool inPlace = doc.getInPlaceUpdates(&damages, &source);

            // If something changed in the document, verify that no immutable fields were changed
            // and data is valid for storage.
            if ((!inPlace || !damages.empty()) ) {
                if (!(request.isFromReplication() || request.isFromMigration())) {
                    const std::vector<FieldRef*>* immutableFields = NULL;
                    if (const UpdateLifecycle* lifecycle = request.getLifecycle())
                        immutableFields = lifecycle->getImmutableFields();

                    uassertStatusOK(validate(oldObj,
                                             updatedFields,
                                             doc,
                                             immutableFields,
                                             driver->modOptions()) );
                }
            }

            // Save state before making changes
            runner->saveState();

            if (inPlace && !driver->modsAffectIndices()) {

                // If a set of modifiers were all no-ops, we are still 'in place', but there is
                // no work to do, in which case we want to consider the object unchanged.
                if (!damages.empty() ) {

                    collection->details()->paddingFits();

                    // All updates were in place. Apply them via durability and writing pointer.
                    mutablebson::DamageVector::const_iterator where = damages.begin();
                    const mutablebson::DamageVector::const_iterator end = damages.end();
                    for( ; where != end; ++where ) {
                        const char* sourcePtr = source + where->sourceOffset;
                        void* targetPtr = getDur().writingPtr(
                            const_cast<char*>(oldObj.objdata()) + where->targetOffset,
                            where->size);
                        std::memcpy(targetPtr, sourcePtr, where->size);
                    }
                    objectWasChanged = true;
                    opDebug->fastmod = true;
                }
                newObj = oldObj;
            }
            else {

                // The updates were not in place. Apply them through the file manager.
                newObj = doc.getObject();
                StatusWith<DiskLoc> res = collection->updateDocument(loc,
                                                                     newObj,
                                                                     true,
                                                                     opDebug);
                uassertStatusOK(res.getStatus());
                DiskLoc newLoc = res.getValue();

                // If we are tracking updated DiskLocs because we are doing a multi-update, and
                // if we've moved this object to a new location, make sure we don't apply that
                // update again if our traversal picks the object again. NOTE: The runner takes
                // care of deduping non-moved docs.
                if (updatedLocs && (newLoc != loc)) {
                    updatedLocs->insert(newLoc);
                }

                objectWasChanged = true;
            }

            // Restore state after modification
            uassert(17278,
                    "Update could not restore runner state after updating a document.",
                    runner->restoreState());

            // Call logOp if requested.
            if (request.shouldCallLogOp() && !logObj.isEmpty()) {
                BSONObj idQuery = driver->makeOplogEntryQuery(newObj, request.isMulti());
                logOp("u", nsString.ns().c_str(), logObj , &idQuery,
                      NULL, request.isFromMigration(), &newObj);
            }

            // If it was noop since the document didn't change, record that.
            if (!objectWasChanged)
                opDebug->nupdateNoops++;

            if (!request.isMulti()) {
                break;
            }

            // Opportunity for journaling to write during the update.
            getDur().commitIfNeeded();

            // Disable yielding if isolate with write done
            const int numChanged = numMatched - opDebug->nupdateNoops;

            // See if we have a write in isolation mode
            const bool isolationModeWriteOccured = isolated && (numChanged > 0);

            // Change to manual yielding (no yielding) if we have written in isolation mode
            if (isolationModeWriteOccured) {
                runner->setYieldPolicy(Runner::YIELD_MANUAL);
            }

            // Keep track of yield count so we can see if one happens on the getNext() call
            const int oldYieldCount = curOp->numYields();

            // Get next doc, and location
            state = runner->getNext(&oldObj, &loc);

            // Refresh things after a yield.
            const bool didYield = (oldYieldCount != curOp->numYields());
            if (!isolationModeWriteOccured && didYield) {

                // We yielded and recovered OK, and our cursor is still good. Details about
                // our namespace may have changed while we were yielded, so we re-acquire
                // them here. If we can't do so, escape the update loop. Otherwise, refresh
                // the driver so that it knows about what is currently indexed.
                const UpdateLifecycle* lifecycle = request.getLifecycle();
                collection = cc().database()->getCollection(nsString.ns());
                if (!collection || (lifecycle && !lifecycle->canContinue())) {
                    uasserted(17270,
                              "Update aborted due to invalid state transitions after yield.");
                }

                if (lifecycle && lifecycle->canContinue()) {
                    IndexPathSet indexes;
                    lifecycle->getIndexKeys(&indexes);
                    driver->refreshIndexKeys(indexes);
                }
            }
        }

        // TODO: Can this be simplified?
        if ((numMatched > 0) || (numMatched == 0 && !request.isUpsert()) ) {
            opDebug->nMatched = numMatched;
            return UpdateResult(numMatched > 0 /* updated existing object(s) */,
                                !driver->isDocReplacement() /* $mod or obj replacement */,
                                numMatched /* # of docments update, even no-ops */,
                                BSONObj());
        }

        //
        // We haven't found any existing document so an insert is done
        // (upsert is true).
        //
        opDebug->upsert = true;

        // Since this is an insert (no docs found and upsert:true), we will be logging it
        // as an insert in the oplog. We don't need the driver's help to build the
        // oplog record, then. We also set the context of the update driver to the INSERT_CONTEXT.
        // Some mods may only work in that context (e.g. $setOnInsert).
        driver->setLogOp(false);
        driver->setContext(ModifierInterface::ExecInfo::INSERT_CONTEXT);

        // Reset the document we will be writing to
        doc.reset();

        // This remains the empty object in the case of an object replacement, but in the case
        // of an upsert where we are creating a base object from the query and applying mods,
        // we capture the query as the original so that we can detect immutable field mutations.
        BSONObj original = BSONObj();

        // Calling createFromQuery will populate the 'doc' with fields from the query which
        // creates the base of the update for the inserterd doc (because upsert was true)
        uassertStatusOK(driver->populateDocumentWithQueryFields(cq, doc));
        if (!driver->isDocReplacement()) {
            opDebug->fastmodinsert = true;
            // We need all the fields from the query to compare against for validation below.
            original = doc.getObject();
        }
        else {
            original = request.getQuery();
        }

        // Apply the update modifications and then log the update as an insert manually.
        FieldRefSet updatedFields;
        Status status = driver->update(StringData(), &doc, NULL, &updatedFields);
        if (!status.isOK()) {
            uasserted(16836, status.reason());
        }

        // Ensure _id exists and is first
        uassertStatusOK(ensureIdAndFirst(doc));

        // Validate that the object replacement or modifiers resulted in a document
        // that contains all the immutable keys and can be stored.
        if (!(request.isFromReplication() || request.isFromMigration())){
            const std::vector<FieldRef*>* immutableFields = NULL;
            if (const UpdateLifecycle* lifecycle = request.getLifecycle())
                immutableFields = lifecycle->getImmutableFields();

            uassertStatusOK(validate(original,
                                     updatedFields,
                                     doc,
                                     immutableFields,
                                     driver->modOptions()) );
        }

        // Only create the collection if the doc will be inserted.
        if (!collection) {
            collection = cc().database()->getCollection(request.getNamespaceString().ns());
            if (!collection) {
                collection = cc().database()->createCollection(request.getNamespaceString().ns());
            }
        }

        // Insert the doc
        BSONObj newObj = doc.getObject();
        StatusWith<DiskLoc> newLoc = collection->insertDocument(newObj,
                                                                !request.isGod() /*enforceQuota*/);
        uassertStatusOK(newLoc.getStatus());
        if (request.shouldCallLogOp()) {
            logOp("i", nsString.ns().c_str(), newObj,
                   NULL, NULL, request.isFromMigration(), &newObj);
        }

        opDebug->nMatched = 1;
        return UpdateResult(false /* updated a non existing document */,
                            !driver->isDocReplacement() /* $mod or obj replacement? */,
                            1 /* count of updated documents */,
                            newObj /* object that was upserted */ );
    }

    BSONObj applyUpdateOperators(const BSONObj& from, const BSONObj& operators) {
        UpdateDriver::Options opts;
        UpdateDriver driver(opts);
        Status status = driver.parse(operators);
        if (!status.isOK()) {
            uasserted(16838, status.reason());
        }

        mutablebson::Document doc(from, mutablebson::Document::kInPlaceDisabled);
        status = driver.update(StringData(), &doc);
        if (!status.isOK()) {
            uasserted(16839, status.reason());
        }

        return doc.getObject();
    }

}  // namespace mongo
