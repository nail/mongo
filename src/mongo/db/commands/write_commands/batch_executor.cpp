/**
 *    Copyright (C) 2013 10gen Inc.
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

#include "mongo/db/commands/write_commands/batch_executor.h"

#include "mongo/db/commands.h"
#include "mongo/db/instance.h"
#include "mongo/db/introspect.h"
#include "mongo/db/lasterror.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/ops/update.h"
#include "mongo/db/ops/update_lifecycle_impl.h"
#include "mongo/db/pagefault.h"
#include "mongo/db/stats/counters.h"

namespace mongo {

    WriteBatchExecutor::WriteBatchExecutor(Client* client, OpCounters* opCounters, LastError* le)
        : _client(client)
        , _opCounters(opCounters)
        , _le(le) {}

    bool WriteBatchExecutor::executeBatch(const WriteBatch& writeBatch,
                                          string* errMsg,
                                          BSONObjBuilder* result) {
        Timer commandTimer;

        BSONArrayBuilder resultsArray;
        bool batchSuccess = applyWriteBatch(writeBatch, &resultsArray);
        result->append("resultsBatchSuccess", batchSuccess);
        result->append("results", resultsArray.arr());

        BSONObjBuilder writeConcernResults;
        Timer writeConcernTimer;

        // TODO Define final layout for write commands result object.

        // bool writeConcernSuccess = waitForWriteConcern(writeBatch.getWriteConcern(),
        //                                                writeConcernResults,
        //                                                !batchSuccess,
        //                                                *errMsg);
        // if (!writeConcernSuccess) {
        //     return false;
        // }
        //
        // Try to enforce the write concern if everything succeeded (unordered or ordered)
        // OR if something succeeded and we're unordered.
        //

        auto_ptr<WCErrorDetail> wcError;
        bool needToEnforceWC = writeErrors.empty()
                               || ( !request.getOrdered()
                                    && writeErrors.size() < request.sizeWriteOps() );

        if ( needToEnforceWC ) {

            _client->curop()->setMessage( "waiting for write concern" );

            WriteConcernResult res;
            status = waitForWriteConcern( writeConcern, _client->getLastOp(), &res );

            if ( !status.isOK() ) {
                wcError.reset( toWriteConcernError( status, res ) );
            }
        }

        //
        // Refresh metadata if needed
        //

        bool staleBatch = !writeErrors.empty()
                          && writeErrors.back()->getErrCode() == ErrorCodes::StaleShardVersion;

        if ( staleBatch ) {

            const BatchedRequestMetadata* requestMetadata = request.getMetadata();
            dassert( requestMetadata );

            // Make sure our shard name is set or is the same as what was set previously
            if ( shardingState.setShardName( requestMetadata->getShardName() ) ) {

                //
                // First, we refresh metadata if we need to based on the requested version.
                //

                ChunkVersion latestShardVersion;
                shardingState.refreshMetadataIfNeeded( request.getTargetingNS(),
                                                       requestMetadata->getShardVersion(),
                                                       &latestShardVersion );

                // Report if we're still changing our metadata
                // TODO: Better reporting per-collection
                if ( shardingState.inCriticalMigrateSection() ) {
                    noteInCriticalSection( writeErrors.back() );
                }

                if ( queueForMigrationCommit ) {

                    //
                    // Queue up for migration to end - this allows us to be sure that clients will
                    // not repeatedly try to refresh metadata that is not yet written to the config
                    // server.  Not necessary for correctness.
                    // Exposed as optional parameter to allow testing of queuing behavior with
                    // different network timings.
                    //

                    const ChunkVersion& requestShardVersion = requestMetadata->getShardVersion();

                    //
                    // Only wait if we're an older version (in the current collection epoch) and
                    // we're not write compatible, implying that the current migration is affecting
                    // writes.
                    //

                    if ( requestShardVersion.isOlderThan( latestShardVersion ) &&
                         !requestShardVersion.isWriteCompatibleWith( latestShardVersion ) ) {

                        while ( shardingState.inCriticalMigrateSection() ) {

                            log() << "write request to old shard version "
                                  << requestMetadata->getShardVersion().toString()
                                  << " waiting for migration commit" << endl;

                            shardingState.waitTillNotInCriticalSection( 10 /* secs */);
                        }
                    }
                }
            }
            else {
                // If our shard name is stale, our version must have been stale as well
                dassert( writeErrors.size() == request.sizeWriteOps() );
            }
        }

        //
        // Construct response
        //

        response->setOk( true );

        if ( !silentWC ) {

            if ( upserted.size() ) {
                response->setUpsertDetails( upserted );
                upserted.clear();
            }

            if ( writeErrors.size() ) {
                response->setErrDetails( writeErrors );
                writeErrors.clear();
            }

            if ( wcError.get() ) {
                response->setWriteConcernError( wcError.release() );
            }

            if ( anyReplEnabled() ) {
                response->setLastOp( _client->getLastOp() );
                if (theReplSet) {
                    response->setElectionId( theReplSet->getElectionId() );
                }
            }

            // Set the stats for the response
            response->setN( _stats->numInserted + _stats->numUpserted + _stats->numMatched
                            + _stats->numDeleted );
            if ( request.getBatchType() == BatchedCommandRequest::BatchType_Update )
                response->setNModified( _stats->numModified );
        }

        dassert( response->isValid( NULL ) );
    }

    // Translates write item type to wire protocol op code.
    // Helper for WriteBatchExecutor::applyWriteItem().
    static int getOpCode( BatchedCommandRequest::BatchType writeType ) {
        switch ( writeType ) {
        case BatchedCommandRequest::BatchType_Insert:
            return dbInsert;
        case BatchedCommandRequest::BatchType_Update:
            return dbUpdate;
        default:
            dassert( writeType == BatchedCommandRequest::BatchType_Delete );
            return dbDelete;
        }
        return 0;
    }

    static void buildStaleError( const ChunkVersion& shardVersionRecvd,
                                 const ChunkVersion& shardVersionWanted,
                                 WriteErrorDetail* error ) {

        // Write stale error to results
        error->setErrCode( ErrorCodes::StaleShardVersion );

        BSONObjBuilder infoB;
        shardVersionWanted.addToBSON( infoB, "vWanted" );
        error->setErrInfo( infoB.obj() );

        string errMsg = stream() << "stale shard version detected before write, received "
                                 << shardVersionRecvd.toString() << " but local version is "
                                 << shardVersionWanted.toString();
        error->setErrMessage( errMsg );
    }

    static bool checkShardVersion( ShardingState* shardingState,
                                   const BatchedCommandRequest& request,
                                   WriteErrorDetail** error ) {

        const NamespaceString nss( request.getTargetingNS() );
        Lock::assertWriteLocked( nss.ns() );

        ChunkVersion requestShardVersion =
            request.isMetadataSet() && request.getMetadata()->isShardVersionSet() ?
                request.getMetadata()->getShardVersion() : ChunkVersion::IGNORED();

        if ( shardingState->enabled() ) {

            CollectionMetadataPtr metadata = shardingState->getCollectionMetadata( nss.ns() );

            if ( !ChunkVersion::isIgnoredVersion( requestShardVersion ) ) {

                ChunkVersion shardVersion =
                    metadata ? metadata->getShardVersion() : ChunkVersion::UNSHARDED();

        result->append("micros", static_cast<long long>(commandTimer.micros()));

        return true;
    }

    bool WriteBatchExecutor::applyWriteBatch(const WriteBatch& writeBatch,
                                             BSONArrayBuilder* resultsArray) {
        bool batchSuccess = true;
        for (size_t i = 0; i < writeBatch.getNumWriteItems(); ++i) {
            const WriteBatch::WriteItem& writeItem = writeBatch.getWriteItem(i);

            // All writes in the batch must be of the same type:
            dassert(writeBatch.getWriteType() == writeItem.getWriteType());

            BSONObjBuilder results;
            bool opSuccess = applyWriteItem(writeBatch.getNS(), writeItem, &results);
            resultsArray->append(results.obj());

            batchSuccess &= opSuccess;

        currentOp->debug().ns = currentOp->getNS();
        currentOp->debug().op = currentOp->getOp();

        if ( currWrite.getOpType() == BatchedCommandRequest::BatchType_Insert ) {
            // No-op for insert, we don't update query or updateobj
        }
        else if ( currWrite.getOpType() == BatchedCommandRequest::BatchType_Update ) {
            currentOp->setQuery( currWrite.getUpdate()->getQuery() );
            currentOp->debug().query = currWrite.getUpdate()->getQuery();
            currentOp->debug().updateobj = currWrite.getUpdate()->getUpdateExpr();
        }
        else {
            dassert( currWrite.getOpType() == BatchedCommandRequest::BatchType_Delete );
            currentOp->setQuery( currWrite.getDelete()->getQuery() );
            currentOp->debug().query = currWrite.getDelete()->getQuery();
        }

        return currentOp.release();
    }

    void WriteBatchExecutor::incOpStats( const BatchItemRef& currWrite ) {

        if ( currWrite.getOpType() == BatchedCommandRequest::BatchType_Insert ) {
            // No-op, for inserts we increment not on the op but once for each write
        }
        else if ( currWrite.getOpType() == BatchedCommandRequest::BatchType_Update ) {
            _opCounters->gotUpdate();
        }
        else {
            dassert( currWrite.getOpType() == BatchedCommandRequest::BatchType_Delete );
            _opCounters->gotDelete();
        }
    }

    void WriteBatchExecutor::incWriteStats( const BatchItemRef& currWrite,
                                            const WriteOpStats& stats,
                                            const WriteErrorDetail* error,
                                            CurOp* currentOp ) {

        if ( currWrite.getOpType() == BatchedCommandRequest::BatchType_Insert ) {
            // We increment batch inserts like individual inserts
            _opCounters->gotInsert();
            _stats->numInserted += stats.n;
            _le->nObjects = stats.n;
            currentOp->debug().ninserted += stats.n;
        }
        else if ( currWrite.getOpType() == BatchedCommandRequest::BatchType_Update ) {
            if ( stats.upsertedID.isEmpty() ) {
                _stats->numMatched += stats.n;
                _stats->numModified += stats.nModified;
            }
            else {
                ++_stats->numUpserted;
            }

            if ( !error ) {
                _le->recordUpdate( stats.upsertedID.isEmpty() && stats.n > 0,
                        stats.n,
                        stats.upsertedID );
            }
        }
        else {
            dassert( currWrite.getOpType() == BatchedCommandRequest::BatchType_Delete );
            _stats->numDeleted += stats.n;
            if ( !error ) {
                _le->recordDelete( stats.n );
            }
        }

        return batchSuccess;
    }

    namespace {

        // Translates write item type to wire protocol op code.
        // Helper for WriteBatchExecutor::applyWriteItem().
        int getOpCode(WriteBatch::WriteType writeType) {
            switch (writeType) {
            case WriteBatch::WRITE_INSERT:
                return dbInsert;
            case WriteBatch::WRITE_UPDATE:
                return dbUpdate;
            case WriteBatch::WRITE_DELETE:
                return dbDelete;
            }
            dassert(false);
            return 0;
        }

    } // namespace

    bool WriteBatchExecutor::applyWriteItem(const string& ns,
                                            const WriteBatch::WriteItem& writeItem,
                                            BSONObjBuilder* results) {
        // Clear operation's LastError before starting.
        _le->reset(true);

        uint64_t itemTimeMicros = 0;
        bool opSuccess = true;

        // Each write operation executes in its own PageFaultRetryableSection.  This means that
        // a single batch can throw multiple PageFaultException's, which is not the case for
        // other operations.
        PageFaultRetryableSection s;
        while (true) {
            try {
                // Execute the write item as a child operation of the current operation.
                CurOp childOp(_client, _client->curop());

                // TODO Modify CurOp "wrapped" constructor to take an opcode, so calling .reset()
                // is unneeded
                childOp.reset(_client->getRemote(), getOpCode(writeItem.getWriteType()));

                childOp.ensureStarted();
                OpDebug& opDebug = childOp.debug();
                opDebug.ns = ns;
                {
                    Client::WriteContext ctx(ns);

                    switch(writeItem.getWriteType()) {
                    case WriteBatch::WRITE_INSERT:
                        opSuccess = applyInsert(ns, writeItem, &childOp);
                        break;
                    case WriteBatch::WRITE_UPDATE:
                        opSuccess = applyUpdate(ns, writeItem, &childOp);
                        break;
                    case WriteBatch::WRITE_DELETE:
                        opSuccess = applyDelete(ns, writeItem, &childOp);
                        break;
                    }
                }
                childOp.done();
                itemTimeMicros = childOp.totalTimeMicros();

                opDebug.executionTime = childOp.totalTimeMillis();
                opDebug.recordStats();

                // Log operation if running with at least "-v", or if exceeds slow threshold.
                if (logger::globalLogDomain()->shouldLog(logger::LogSeverity::Debug(1))
                    || opDebug.executionTime > cmdLine.slowMS + childOp.getExpectedLatencyMs()) {

                    MONGO_TLOG(1) << opDebug.report(childOp) << endl;
                }

                // TODO Log operation if logLevel >= 3 and assertion thrown (as assembleResponse()
                // does).

                // Save operation to system.profile if shouldDBProfile().
                if (childOp.shouldDBProfile(opDebug.executionTime)) {
                    profile(*_client, getOpCode(writeItem.getWriteType()), childOp);
                }
                break;
            }
            catch (PageFaultException& e) {
                e.touch();
            }
        }

        // Fill caller's builder with results of operation, using LastError.
        results->append("ok", opSuccess);
        _le->appendSelf(*results, false);
        results->append("micros", static_cast<long long>(itemTimeMicros));

        return opSuccess;
    }

    bool WriteBatchExecutor::applyInsert(const string& ns,
                                         const WriteBatch::WriteItem& writeItem,
                                         CurOp* currentOp) {
        OpDebug& opDebug = currentOp->debug();

        _opCounters->gotInsert();

        opDebug.op = dbInsert;

        BSONObj doc;

        string errMsg;
        bool ret = writeItem.parseInsertItem(&errMsg, &doc);
        verify(ret); // writeItem should have been already validated by WriteBatch::parse().

        try {
            // TODO Should call insertWithObjMod directly instead of checkAndInsert?  Note that
            // checkAndInsert will use mayInterrupt=false, so index builds initiated here won't
            // be interruptible.
            checkAndInsert(ns.c_str(), doc);
            getDur().commitIfNeeded();
            _le->nObjects = 1; // TODO Replace after implementing LastError::recordInsert().
            opDebug.ninserted = 1;
        }
        catch (UserException& e) {
            opDebug.exceptionInfo = e.getInfo();
            return false;
        }

        return true;
    }

    bool WriteBatchExecutor::applyUpdate(const string& ns,
                                         const WriteBatch::WriteItem& writeItem,
                                         CurOp* currentOp) {
        OpDebug& opDebug = currentOp->debug();

        _opCounters->gotUpdate();

        BSONObj queryObj;
        BSONObj updateObj;
        bool multi;
        bool upsert;

        string errMsg;
        bool ret = writeItem.parseUpdateItem(&errMsg, &queryObj, &updateObj, &multi, &upsert);
        verify(ret); // writeItem should have been already validated by WriteBatch::parse().

        currentOp->setQuery(queryObj);
        opDebug.op = dbUpdate;
        opDebug.query = queryObj;

        bool resExisting = false;
        long long resNum = 0;
        BSONObj resUpserted;
        try {

            const NamespaceString requestNs(ns);
            UpdateRequest request(requestNs);

            request.setQuery(queryObj);
            request.setUpdates(updateObj);
            request.setUpsert(upsert);
            request.setMulti(multi);
            request.setUpdateOpLog();
            // TODO(greg) We need to send if we are ignoring the shard version below, but for now yes
            UpdateLifecycleImpl updateLifecycle(true, requestNs);
            request.setLifecycle(&updateLifecycle);

            UpdateResult res = update(request, &opDebug);

            resExisting = res.existing;
            resNum = res.numMatched;
            resUpserted = res.upserted;

            stats->numUpdated += resUpserted.isEmpty() ? resNum : 0;
            stats->numUpserted += !resUpserted.isEmpty() ? 1 : 0;
        }
        catch (UserException& e) {
            opDebug.exceptionInfo = e.getInfo();
            return false;
        }

        _le->recordUpdate(resExisting, resNum, resUpserted);

        return true;
    }

    bool WriteBatchExecutor::applyDelete(const string& ns,
                                         const WriteBatch::WriteItem& writeItem,
                                         CurOp* currentOp) {
        OpDebug& opDebug = currentOp->debug();

        _opCounters->gotDelete();

        BSONObj queryObj;

        string errMsg;
        bool ret = writeItem.parseDeleteItem(&errMsg, &queryObj);
        verify(ret); // writeItem should have been already validated by WriteBatch::parse().

        currentOp->setQuery(queryObj);
        opDebug.op = dbDelete;
        opDebug.query = queryObj;

        long long n;

        try {
            n = deleteObjects(ns.c_str(),
                              queryObj,
                              /*justOne*/false,
                              /*logOp*/true,
                              /*god*/false,
                              /*rs*/NULL);
        }
        catch (UserException& e) {
            opDebug.exceptionInfo = e.getInfo();
            return false;
        }

        _le->recordDelete(n);
        opDebug.ndeleted = n;

        return true;
    }

} // namespace mongo
