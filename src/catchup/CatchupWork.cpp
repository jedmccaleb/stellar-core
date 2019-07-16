// Copyright 2017 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "catchup/CatchupWork.h"
#include "catchup/ApplyBucketsWork.h"
#include "catchup/ApplyLedgerChainWork.h"
#include "catchup/CatchupConfiguration.h"
#include "catchup/VerifyLedgerChainWork.h"
#include "history/FileTransferInfo.h"
#include "history/HistoryManager.h"
#include "historywork/BatchDownloadWork.h"
#include "historywork/DownloadBucketsWork.h"
#include "historywork/GetAndUnzipRemoteFileWork.h"
#include "historywork/GetHistoryArchiveStateWork.h"
#include "historywork/VerifyBucketWork.h"
#include "ledger/LedgerManager.h"
#include "main/Application.h"
#include "util/Logging.h"
#include <lib/util/format.h>

namespace stellar
{

CatchupWork::CatchupWork(Application& app,
                         CatchupConfiguration catchupConfiguration,
                         ProgressHandler progressHandler)
    : Work(app, "catchup", BasicWork::RETRY_NEVER)
    , mLocalState{app.getHistoryManager().getLastClosedHistoryArchiveState()}
    , mDownloadDir{std::make_unique<TmpDir>(
          mApp.getTmpDirManager().tmpDir(getName()))}
    , mCatchupConfiguration{catchupConfiguration}
    , mProgressHandler{progressHandler}
{
}

CatchupWork::~CatchupWork()
{
}

std::string
CatchupWork::getStatus() const
{
    if (mCatchupSeq)
    {
        return mCatchupSeq->getStatus();
    }
    return BasicWork::getStatus();
}

void
CatchupWork::doReset()
{
    mBucketsAppliedEmitted = false;
    mBuckets.clear();
    mDownloadVerifyLedgersSeq.reset();
    mBucketVerifyApplySeq.reset();
    mTransactionsVerifyApplySeq.reset();
    mGetHistoryArchiveStateWork.reset();
    auto const& lcl = mApp.getLedgerManager().getLastClosedLedgerHeader();
    mLastClosedLedgerHashPair =
        LedgerNumHashPair(lcl.header.ledgerSeq, make_optional<Hash>(lcl.hash));
    mCatchupSeq.reset();
    mGetBucketStateWork.reset();
    mRemoteState = {};
    mApplyBucketsRemoteState = {};
    mLastApplied = mApp.getLedgerManager().getLastClosedLedgerHeader();
}

bool
CatchupWork::hasAnyLedgersToCatchupTo() const
{
    assert(mGetHistoryArchiveStateWork);
    assert(mGetHistoryArchiveStateWork->getState() == State::WORK_SUCCESS);

    return mLastClosedLedgerHashPair.first <= mRemoteState.currentLedger;
}

void
CatchupWork::downloadVerifyLedgerChain(CatchupRange const& catchupRange,
                                       LedgerNumHashPair rangeEnd)
{
    auto verifyRange = LedgerRange{catchupRange.mApplyBuckets
                                       ? catchupRange.getBucketApplyLedger()
                                       : catchupRange.mLedgers.mFirst,
                                   catchupRange.getLast()};
    auto checkpointRange =
        CheckpointRange{verifyRange, mApp.getHistoryManager()};
    auto getLedgers = std::make_shared<BatchDownloadWork>(
        mApp, checkpointRange, HISTORY_FILE_TYPE_LEDGER, *mDownloadDir);
    mVerifyLedgers = std::make_shared<VerifyLedgerChainWork>(
        mApp, *mDownloadDir, verifyRange, mLastClosedLedgerHashPair, rangeEnd);

    std::vector<std::shared_ptr<BasicWork>> seq{getLedgers, mVerifyLedgers};
    mDownloadVerifyLedgersSeq =
        addWork<WorkSequence>("download-verify-ledgers-seq", seq);
}

bool
CatchupWork::alreadyHaveBucketsHistoryArchiveState(uint32_t atCheckpoint) const
{
    return atCheckpoint == mRemoteState.currentLedger;
}

WorkSeqPtr
CatchupWork::downloadApplyBuckets()
{
    std::vector<std::string> hashes =
        mApplyBucketsRemoteState.differingBuckets(mLocalState);
    auto getBuckets = std::make_shared<DownloadBucketsWork>(
        mApp, mBuckets, hashes, *mDownloadDir);
    auto applyBuckets = std::make_shared<ApplyBucketsWork>(
        mApp, mBuckets, mApplyBucketsRemoteState,
        mVerifiedLedgerRangeStart.header.ledgerVersion);

    std::vector<std::shared_ptr<BasicWork>> seq{getBuckets, applyBuckets};
    return std::make_shared<WorkSequence>(mApp, "download-verify-apply-buckets",
                                          seq, RETRY_NEVER);
}

void
CatchupWork::assertBucketState()
{
    // Consistency check: mRemoteState and mVerifiedLedgerRangeStart should
    // point to the same ledger and the same BucketList.
    assert(mApplyBucketsRemoteState.currentLedger ==
           mVerifiedLedgerRangeStart.header.ledgerSeq);
    assert(mApplyBucketsRemoteState.getBucketListHash() ==
           mVerifiedLedgerRangeStart.header.bucketListHash);

    // Consistency check: LCL should be in the _past_ from
    // firstVerified, since we're about to clobber a bunch of DB
    // state with new buckets held in firstVerified's state.
    auto lcl = mApp.getLedgerManager().getLastClosedLedgerHeader();
    if (mVerifiedLedgerRangeStart.header.ledgerSeq < lcl.header.ledgerSeq)
    {
        throw std::runtime_error(
            fmt::format("Catchup MINIMAL applying ledger earlier than local "
                        "LCL: {:s} < {:s}",
                        LedgerManager::ledgerAbbrev(mVerifiedLedgerRangeStart),
                        LedgerManager::ledgerAbbrev(lcl)));
    }
}

WorkSeqPtr
CatchupWork::downloadApplyTransactions(CatchupRange const& catchupRange)
{
    auto range =
        LedgerRange{catchupRange.mLedgers.mFirst, catchupRange.getLast()};
    auto checkpointRange = CheckpointRange{range, mApp.getHistoryManager()};
    auto getTxs = std::make_shared<BatchDownloadWork>(
        mApp, checkpointRange, HISTORY_FILE_TYPE_TRANSACTIONS, *mDownloadDir);
    auto applyLedgers = std::make_shared<ApplyLedgerChainWork>(
        mApp, *mDownloadDir, range, mLastApplied);

    std::vector<std::shared_ptr<BasicWork>> seq{getTxs, applyLedgers};
    return std::make_shared<WorkSequence>(mApp, "download-apply-transactions",
                                          seq, RETRY_NEVER);
}

BasicWork::State
CatchupWork::doWork()
{
    // Step 1: Get history archive state
    if (!mGetHistoryArchiveStateWork)
    {
        auto toLedger = mCatchupConfiguration.toLedger() == 0
                            ? "CURRENT"
                            : std::to_string(mCatchupConfiguration.toLedger());
        CLOG(INFO, "History")
            << "Starting catchup with configuration:\n"
            << "  lastClosedLedger: "
            << mApp.getLedgerManager().getLastClosedLedgerNum() << "\n"
            << "  toLedger: " << toLedger << "\n"
            << "  count: " << mCatchupConfiguration.count();

        auto toCheckpoint =
            mCatchupConfiguration.toLedger() == CatchupConfiguration::CURRENT
                ? CatchupConfiguration::CURRENT
                : mApp.getHistoryManager().nextCheckpointLedger(
                      mCatchupConfiguration.toLedger() + 1) -
                      1;
        mGetHistoryArchiveStateWork =
            addWork<GetHistoryArchiveStateWork>(mRemoteState, toCheckpoint);
        return State::WORK_RUNNING;
    }
    else if (mGetHistoryArchiveStateWork->getState() != State::WORK_SUCCESS)
    {
        return mGetHistoryArchiveStateWork->getState();
    }

    // Step 2: Compare local and remote states
    if (!hasAnyLedgersToCatchupTo())
    {
        CLOG(INFO, "History") << "*";
        CLOG(INFO, "History")
            << "* Target ledger " << mRemoteState.currentLedger
            << " is not newer than last closed ledger "
            << mLastClosedLedgerHashPair.first << " - nothing to do";

        if (mCatchupConfiguration.toLedger() == CatchupConfiguration::CURRENT)
        {
            CLOG(INFO, "History")
                << "* Wait until next checkpoint before retrying";
        }
        else
        {
            CLOG(INFO, "History") << "* If you really want to catchup to "
                                  << mCatchupConfiguration.toLedger()
                                  << " run stellar-core new-db";
        }

        CLOG(INFO, "History") << "*";

        CLOG(ERROR, "History") << "Nothing to catchup to ";

        return State::WORK_FAILURE;
    }

    auto resolvedConfiguration =
        mCatchupConfiguration.resolve(mRemoteState.currentLedger);
    auto catchupRange =
        CatchupRange{mLastClosedLedgerHashPair.first, resolvedConfiguration,
                     mApp.getHistoryManager()};

    // Step 3: If needed, download archive state for buckets
    if (catchupRange.mApplyBuckets)
    {
        auto applyBucketsAt = catchupRange.getBucketApplyLedger();
        if (!alreadyHaveBucketsHistoryArchiveState(applyBucketsAt))
        {
            if (!mGetBucketStateWork)
            {
                mGetBucketStateWork = addWork<GetHistoryArchiveStateWork>(
                    mApplyBucketsRemoteState, applyBucketsAt);
            }
            if (mGetBucketStateWork->getState() != State::WORK_SUCCESS)
            {
                return mGetBucketStateWork->getState();
            }
        }
        else
        {
            mApplyBucketsRemoteState = mRemoteState;
        }
    }

    // Step 4: Download, verify and apply ledgers, buckets and transactions

    // Bucket and transaction processing has started
    if (mCatchupSeq)
    {
        assert(mDownloadVerifyLedgersSeq);
        assert(mTransactionsVerifyApplySeq || !catchupRange.applyLedgers());

        if (mCatchupSeq->getState() == State::WORK_SUCCESS)
        {
            return State::WORK_SUCCESS;
        }
        else if (mBucketVerifyApplySeq)
        {
            if (mBucketVerifyApplySeq->getState() == State::WORK_SUCCESS &&
                !mBucketsAppliedEmitted)
            {
                mProgressHandler(ProgressState::APPLIED_BUCKETS,
                                 mVerifiedLedgerRangeStart,
                                 mCatchupConfiguration.mode());
                mBucketsAppliedEmitted = true;
                mLastApplied =
                    mApp.getLedgerManager().getLastClosedLedgerHeader();
            }
        }
        return mCatchupSeq->getState();
    }
    // Still waiting for ledger headers
    else if (mDownloadVerifyLedgersSeq)
    {
        if (mDownloadVerifyLedgersSeq->getState() == State::WORK_SUCCESS)
        {
            mVerifiedLedgerRangeStart =
                mVerifyLedgers->getVerifiedLedgerRangeStart();
            if (catchupRange.mApplyBuckets && !mBucketsAppliedEmitted)
            {
                assertBucketState();
            }

            std::vector<std::shared_ptr<BasicWork>> seq;
            if (catchupRange.mApplyBuckets)
            {
                // Step 4.2: Download, verify and apply buckets
                mBucketVerifyApplySeq = downloadApplyBuckets();
                seq.push_back(mBucketVerifyApplySeq);
            }

            if (catchupRange.applyLedgers())
            {
                // Step 4.3: Download and apply ledger chain
                mTransactionsVerifyApplySeq =
                    downloadApplyTransactions(catchupRange);
                seq.push_back(mTransactionsVerifyApplySeq);
            }

            mCatchupSeq =
                addWork<WorkSequence>("catchup-seq", seq, RETRY_NEVER);
            return State::WORK_RUNNING;
        }
        return mDownloadVerifyLedgersSeq->getState();
    }

    // Step 4.1: Download and verify ledger chain
    downloadVerifyLedgerChain(catchupRange,
                              LedgerNumHashPair(catchupRange.getLast(),
                                                mCatchupConfiguration.hash()));

    return State::WORK_RUNNING;
}

void
CatchupWork::onFailureRaise()
{
    CLOG(WARNING, "History") << "Catchup failed";

    mApp.getCatchupManager().historyCaughtup();
    mProgressHandler(ProgressState::FAILED, LedgerHeaderHistoryEntry{},
                     mCatchupConfiguration.mode());
    Work::onFailureRaise();
}

void
CatchupWork::onSuccess()
{
    CLOG(INFO, "History") << "Catchup finished";

    mProgressHandler(ProgressState::APPLIED_TRANSACTIONS, mLastApplied,
                     mCatchupConfiguration.mode());
    mProgressHandler(ProgressState::FINISHED, mLastApplied,
                     mCatchupConfiguration.mode());
    mApp.getCatchupManager().historyCaughtup();
    Work::onSuccess();
}

namespace
{

CatchupRange::Ledgers
computeCatchupledgers(uint32_t lastClosedLedger,
                      CatchupConfiguration const& configuration,
                      HistoryManager const& historyManager)
{
    if (lastClosedLedger == 0)
    {
        throw std::invalid_argument{"lastClosedLedger == 0"};
    }

    if (configuration.toLedger() <= lastClosedLedger)
    {
        throw std::invalid_argument{
            "configuration.toLedger() <= lastClosedLedger"};
    }

    if (configuration.toLedger() == CatchupConfiguration::CURRENT)
    {
        throw std::invalid_argument{
            "configuration.toLedger() == CatchupConfiguration::CURRENT"};
    }

    // do a complete catchup if not starting from new-db
    if (lastClosedLedger > LedgerManager::GENESIS_LEDGER_SEQ)
    {
        return {lastClosedLedger + 1,
                configuration.toLedger() - lastClosedLedger};
    }

    // do a complete catchup if count is big enough
    if (configuration.count() >=
        configuration.toLedger() - LedgerManager::GENESIS_LEDGER_SEQ)
    {
        return {LedgerManager::GENESIS_LEDGER_SEQ + 1,
                configuration.toLedger() - LedgerManager::GENESIS_LEDGER_SEQ};
    }

    auto smallestLedgerToApply =
        configuration.toLedger() - std::max(1u, configuration.count()) + 1;

    // checkpoint that contains smallestLedgerToApply - it is first one than
    // can be applied, it is always greater than LCL
    auto firstCheckpoint = historyManager.checkpointContainingLedger(1);
    auto smallestCheckpointToApply =
        historyManager.checkpointContainingLedger(smallestLedgerToApply);

    // if first ledger that should be applied is on checkpoint boundary then
    // we do an bucket-apply, and apply ledgers from netx one
    if (smallestCheckpointToApply == smallestLedgerToApply)
    {
        return {smallestLedgerToApply + 1,
                configuration.toLedger() - smallestLedgerToApply};
    }

    // we are before first checkpoint - full catchup is required
    if (smallestCheckpointToApply == firstCheckpoint)
    {
        return {LedgerManager::GENESIS_LEDGER_SEQ + 1,
                configuration.toLedger() - LedgerManager::GENESIS_LEDGER_SEQ};
    }

    // need one more checkpoint to ensure that smallestLedgerToApply has history
    // entry
    return {smallestCheckpointToApply -
                historyManager.getCheckpointFrequency() + 1,
            configuration.toLedger() - smallestCheckpointToApply +
                historyManager.getCheckpointFrequency()};
}
}

CatchupRange::CatchupRange(uint32_t lastClosedLedger,
                           CatchupConfiguration const& configuration,
                           HistoryManager const& historyManager)
    : mLedgers{computeCatchupledgers(lastClosedLedger, configuration,
                                     historyManager)}
    , mApplyBuckets{mLedgers.mFirst > lastClosedLedger + 1}
{
}

uint32_t
CatchupRange::getLast() const
{
    return mLedgers.mFirst + mLedgers.mCount - 1;
}

uint32_t
CatchupRange::getBucketApplyLedger() const
{
    if (!mApplyBuckets)
    {
        throw std::logic_error("getBucketApplyLedger() cannot be called on "
                               "CatchupRange when mApplyBuckets == false");
    }

    return mLedgers.mFirst - 1;
}
}
