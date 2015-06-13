// Copyright 2014 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "herder/HerderImpl.h"
#include "crypto/Hex.h"
#include "crypto/SHA.h"
#include "crypto/Base58.h"
#include "herder/TxSetFrame.h"
#include "ledger/LedgerManager.h"
#include "main/Application.h"
#include "main/Config.h"
#include "overlay/OverlayManager.h"
#include "scp/Slot.h"
#include "util/Logging.h"
#include "util/Timer.h"
#include "util/make_unique.h"
#include "lib/json/json.h"
#include "scp/Node.h"

#include "medida/meter.h"
#include "medida/counter.h"
#include "medida/metrics_registry.h"
#include "xdrpp/marshal.h"

#include <ctime>

#define MAX_SLOTS_TO_REMEMBER 4

namespace stellar
{

std::unique_ptr<Herder>
Herder::create(Application& app)
{
    return make_unique<HerderImpl>(app);
}

HerderImpl::HerderImpl(Application& app)
    : SCP(app.getConfig().VALIDATION_KEY, app.getConfig().QUORUM_SET)
    , mReceivedTransactions(4)
    , mPendingEnvelopes(app, *this)
    , mTrackingTimer(app)
    , mLastTrigger(app.getClock().now())
    , mTriggerTimer(app)
    , mBumpTimer(app)
    , mNominationTimer(app)
    , mRebroadcastTimer(app)
    , mApp(app)
    , mLedgerManager(app.getLedgerManager())

    , mValueValid(app.getMetrics().NewMeter({"scp", "value", "valid"}, "value"))
    , mValueInvalid(
          app.getMetrics().NewMeter({"scp", "value", "invalid"}, "value"))
    , mValuePrepare(
          app.getMetrics().NewMeter({"scp", "value", "prepare"}, "value"))
    , mValueExternalize(
          app.getMetrics().NewMeter({"scp", "value", "externalize"}, "value"))

    , mBallotValid(
          app.getMetrics().NewMeter({"scp", "ballot", "valid"}, "ballot"))
    , mBallotInvalid(
          app.getMetrics().NewMeter({"scp", "ballot", "invalid"}, "ballot"))
    , mBallotPrepare(
          app.getMetrics().NewMeter({"scp", "ballot", "prepare"}, "ballot"))
    , mBallotPrepared(
          app.getMetrics().NewMeter({"scp", "ballot", "prepared"}, "ballot"))
    , mBallotCommit(
          app.getMetrics().NewMeter({"scp", "ballot", "commit"}, "ballot"))
    , mBallotCommitted(
          app.getMetrics().NewMeter({"scp", "ballot", "committed"}, "ballot"))
    , mBallotSign(
          app.getMetrics().NewMeter({"scp", "ballot", "sign"}, "ballot"))
    , mBallotValidSig(
          app.getMetrics().NewMeter({"scp", "ballot", "validsig"}, "ballot"))
    , mBallotInvalidSig(
          app.getMetrics().NewMeter({"scp", "ballot", "invalidsig"}, "ballot"))
    , mBallotExpire(
          app.getMetrics().NewMeter({"scp", "ballot", "expire"}, "ballot"))

    , mQuorumHeard(
          app.getMetrics().NewMeter({"scp", "quorum", "heard"}, "quorum"))
    , mQsetRetrieve(
          app.getMetrics().NewMeter({"scp", "qset", "retrieve"}, "qset"))

    , mLostSync(app.getMetrics().NewMeter({"scp", "sync", "lost"}, "sync"))

    , mEnvelopeEmit(
          app.getMetrics().NewMeter({"scp", "envelope", "emit"}, "envelope"))
    , mEnvelopeReceive(
          app.getMetrics().NewMeter({"scp", "envelope", "receive"}, "envelope"))
    , mEnvelopeSign(
          app.getMetrics().NewMeter({"scp", "envelope", "sign"}, "envelope"))
    , mEnvelopeValidSig(app.getMetrics().NewMeter(
          {"scp", "envelope", "validsig"}, "envelope"))
    , mEnvelopeInvalidSig(app.getMetrics().NewMeter(
          {"scp", "envelope", "invalidsig"}, "envelope"))

    , mNodeLastAccessSize(
          app.getMetrics().NewCounter({"scp", "memory", "node-last-access"}))
    , mSCPQSetFetchesSize(
          app.getMetrics().NewCounter({"scp", "memory", "qset-fetches"}))
    , mBallotValidationTimersSize(app.getMetrics().NewCounter(
          {"scp", "memory", "ballot-validation-timers"}))

    , mKnownNodesSize(
          app.getMetrics().NewCounter({"scp", "memory", "known-nodes"}))
    , mKnownSlotsSize(
          app.getMetrics().NewCounter({"scp", "memory", "known-slots"}))
    , mCumulativeStatements(app.getMetrics().NewCounter(
          {"scp", "memory", "cumulative-statements"}))
    , mCumulativeCachedQuorumSets(app.getMetrics().NewCounter(
          {"scp", "memory", "cumulative-cached-quorum-sets"}))

{
    Hash hash = sha256(xdr::xdr_to_opaque(app.getConfig().QUORUM_SET));
    mPendingEnvelopes.recvSCPQuorumSet(hash, app.getConfig().QUORUM_SET);
}

HerderImpl::~HerderImpl()
{
}

Herder::State
HerderImpl::getState() const
{
    return mTrackingSCP ? HERDER_TRACKING_STATE : HERDER_SYNCING_STATE;
}

std::string
HerderImpl::getStateHuman() const
{
    static const char* stateStrings[HERDER_NUM_STATE] = {
        "HERDER_SYNCING_STATE", "HERDER_TRACKING_STATE"};
    return std::string(stateStrings[getState()]);
}

void
HerderImpl::bootstrap()
{
    CLOG(INFO, "Herder") << "Force joining SCP with local state";
    assert(!getSecretKey().isZero());
    assert(mApp.getConfig().FORCE_SCP);

    // setup a sufficient state that we can participate in consensus
    auto const& lcl = mLedgerManager.getLastClosedLedgerHeader();
    StellarValue b;
    b.txSetHash = lcl.header.txSetHash;
    b.closeTime = lcl.header.closeTime;
    b.baseFee = mApp.getConfig().DESIRED_BASE_FEE;
    mTrackingSCP = make_unique<ConsensusData>(lcl.header.ledgerSeq, b);
    mLedgerManager.setState(LedgerManager::LM_SYNCED_STATE);

    trackingHeartBeat();
    mLastTrigger = mApp.getClock().now() - Herder::EXP_LEDGER_TIMESPAN_SECONDS;
    ledgerClosed();
}

bool
HerderImpl::validateValue(uint64 slotIndex, uint256 const& nodeID,
                          Value const& value)
{
    StellarValue b;
    try
    {
        xdr::xdr_from_opaque(value, b);
    }
    catch (...)
    {
        mValueInvalid.Mark();
        return false;
    }

    if (!mTrackingSCP)
    {
        // if we're not tracking, there is not much more we can do to validate
        return true;
    }

    // Check slotIndex.
    if (nextConsensusLedgerIndex() > slotIndex)
    {
        // we already moved on from this slot
        // still send it through for emitting the final messages
        return true;
    }
    if (nextConsensusLedgerIndex() < slotIndex)
    {
        // this is probably a bug as "tracking" means we're processing messages
        // only for the right slot
        CLOG(ERROR, "Herder") << "HerderImpl::validateValue"
                              << " i: " << slotIndex
                              << " processing a future message while tracking";

        mValueInvalid.Mark();
        return false;
    }

    // Check closeTime (not too old)
    if (b.closeTime <= mTrackingSCP->mConsensusValue.closeTime)
    {
        mValueInvalid.Mark();
        return false;
    }

    // Check closeTime (not too far in future)
    uint64_t timeNow = mApp.timeNow();
    if (b.closeTime > timeNow + MAX_TIME_SLIP_SECONDS.count())
    {
        mValueInvalid.Mark();
        return false;
    }

    Hash txSetHash = b.txSetHash;

    if (!mLedgerManager.isSynced())
    {
        return true;
    }

    // we are fully synced up

    if (!mLedgerManager.isSynced())
    {
        return true;
    }

    TxSetFramePtr txSet = mPendingEnvelopes.getTxSet(txSetHash);

    bool res;

    if (!txSet)
    {
        CLOG(ERROR, "Herder")
            << "HerderImpl::validateValue"
            << "@" << hexAbbrev(getLocalNodeID()) << " i: " << slotIndex
            << " n: " << hexAbbrev(nodeID) << " txSet not found?";

        this->mValueInvalid.Mark();
        res = false;
    }
    else if (!txSet->checkValid(mApp))
    {
        CLOG(DEBUG, "Herder") << "HerderImpl::validateValue"
                              << " i: " << slotIndex
                              << " n: " << hexAbbrev(nodeID)
                              << " Invalid txSet:"
                              << " " << hexAbbrev(txSet->getContentsHash());
        this->mValueInvalid.Mark();
        res = false;
    }
    else
    {
        CLOG(DEBUG, "Herder")
            << "HerderImpl::validateValue"
            << " i: " << slotIndex << " n: " << hexAbbrev(nodeID) << " txSet:"
            << " " << hexAbbrev(txSet->getContentsHash()) << " OK";
        this->mValueValid.Mark();
        res = true;
    }
    return res;
}

std::string
HerderImpl::getValueString(Value const& v) const
{
    std::ostringstream oss;
    StellarValue b;
    if (v.empty())
    {
        return "[empty]";
    }

    try
    {
        xdr::xdr_from_opaque(v, b);
        uint256 valueHash = sha256(xdr::xdr_to_opaque(b));

        oss << "[ h:" << hexAbbrev(valueHash) << " ]";
        return oss.str();
    }
    catch (...)
    {
        return "[invalid]";
    }
}

bool
HerderImpl::validateBallot(uint64 slotIndex, uint256 const& nodeID,
                           SCPBallot const& ballot)
{
    StellarValue b;
    try
    {
        xdr::xdr_from_opaque(ballot.value, b);
    }
    catch (...)
    {
        mBallotInvalid.Mark();
        return false;
    }

    // Check closeTime (not too far in the future)
    uint64_t timeNow = mApp.timeNow();
    if (b.closeTime > timeNow + MAX_TIME_SLIP_SECONDS.count())
    {
        mBallotInvalid.Mark();
        return false;
    }

    if (mTrackingSCP && nextConsensusLedgerIndex() != slotIndex)
    {
        mValueInvalid.Mark();
        // return cb(false);
        // there is a bug somewhere if we're trying to process messages
        // for a different slot
        throw new std::runtime_error("unexpected state");
    }

    // Check the ballot counter is not growing too rapidly. We ignore ballots
    // that were triggered before the expected series of timeouts (accepting
    // MAX_TIME_SLIP_SECONDS as error). This prevents ballot counter
    // exhaustion attacks.
    uint64_t lastTrigger = VirtualClock::to_time_t(mLastTrigger);
    uint64_t sumTimeouts = 0;
    // The second condition is to prevent attackers from emitting ballots whose
    // verification would busy lock us.
    for (int unsigned i = 0; i < ballot.counter &&
                             (timeNow + MAX_TIME_SLIP_SECONDS.count()) >=
                                 (lastTrigger + sumTimeouts);
         i++)
    {
        sumTimeouts += std::min((long long)MAX_SCP_TIMEOUT_SECONDS.count(),
                                (long long)pow(2.0, i));
    }
    // This inequality is effectively a limitation on `ballot.counter`
    if ((timeNow + MAX_TIME_SLIP_SECONDS.count()) < (lastTrigger + sumTimeouts))
    {
        mBallotInvalid.Mark();
        return false;
    }

    // Check baseFee (within range of desired fee).
    if (b.baseFee < mApp.getConfig().DESIRED_BASE_FEE * .5)
    {
        mBallotInvalid.Mark();
        return false;
    }
    if (b.baseFee > mApp.getConfig().DESIRED_BASE_FEE * 2)
    {
        mBallotInvalid.Mark();
        return false;
    }

    // Ignore ourselves if we're just watching SCP.
    if (getSecretKey().isZero() && nodeID == getLocalNodeID())
    {
        mBallotInvalid.Mark();
        return false;
    }

    uint256 valueHash = sha256(xdr::xdr_to_opaque(ballot.value));

    CLOG(DEBUG, "Herder") << "HerderImpl::validateBallot"
                          << " i: " << slotIndex << " v: " << hexAbbrev(nodeID)
                          << " b: (" << ballot.counter << ","
                          << hexAbbrev(valueHash) << ")";

    mBallotValid.Mark();
    return true;
}

void
HerderImpl::ballotDidHearFromQuorum(uint64 slotIndex, SCPBallot const& ballot)
{
    mQuorumHeard.Mark();
}

void
HerderImpl::ballotGotBumped(uint64 slotIndex, SCPBallot const& ballot,
                            std::chrono::milliseconds timeout)
{
    mBumpTimer.cancel();

    mBumpTimer.expires_from_now(timeout);

    mBumpTimer.async_wait(
        [this, slotIndex, ballot]()
        {
            expireBallot(slotIndex, ballot);
        },
        &VirtualTimer::onFailureNoop);
}

void
HerderImpl::updateSCPCounters()
{
    mKnownNodesSize.set_count(getKnownNodesCount());
    mKnownSlotsSize.set_count(getKnownSlotsCount());
    mCumulativeStatements.set_count(getCumulativeStatemtCount());
}

void
HerderImpl::valueExternalized(uint64 slotIndex, Value const& value)
{
    updateSCPCounters();
    mValueExternalize.Mark();
    mBumpTimer.cancel();
    mNominationTimer.cancel();
    StellarValue b;
    try
    {
        xdr::xdr_from_opaque(value, b);
    }
    catch (...)
    {
        // This may not be possible as all messages are validated and should
        // therefore contain a valid StellarValue.
        CLOG(ERROR, "Herder") << "HerderImpl::valueExternalized"
                              << " Externalized StellarValue malformed";
        // no point in continuing as 'b' contains garbage at this point
        abort();
    }

    Hash const& txSetHash = b.txSetHash;

    CLOG(DEBUG, "Herder") << "HerderImpl::valueExternalized"
                          << " txSet: " << hexAbbrev(txSetHash);

    // current value is not valid anymore
    mCurrentValue.clear();

    mTrackingSCP = make_unique<ConsensusData>(slotIndex, b);
    trackingHeartBeat();

    TxSetFramePtr externalizedSet = mPendingEnvelopes.getTxSet(txSetHash);

    // trigger will be recreated when the ledger is closed
    // we do not want it to trigger while downloading the current set
    // and there is no point in taking a position after the round is over
    mTriggerTimer.cancel();

    // tell the LedgerManager that this value got externalized
    // LedgerManager will perform the proper action based on its internal
    // state: apply, trigger catchup, etc
    LedgerCloseData ledgerData(lastConsensusLedgerIndex(), externalizedSet,
                               b.closeTime, b.baseFee);
    mLedgerManager.externalizeValue(ledgerData);

    // perform cleanups

    // remove all these tx from mReceivedTransactions
    for (auto tx : externalizedSet->mTransactions)
    {
        removeReceivedTx(tx);
    }
    // rebroadcast those left in set 1
    assert(mReceivedTransactions.size() >= 2);
    for (auto& tx : mReceivedTransactions[1])
    {
        auto msg = tx->toStellarMessage();
        mApp.getOverlayManager().broadcastMessage(msg);
    }

    // Evict nodes that weren't touched for more than
    auto now = mApp.getClock().now();
    for (auto& it : mNodeLastAccess)
    {
        if ((now - it.second) > NODE_EXPIRATION_SECONDS)
        {
            purgeNode(it.first);
        }
    }

    // Evict slots that are outside of our ledger validity bracket
    if (slotIndex > MAX_SLOTS_TO_REMEMBER)
    {
        purgeSlots(slotIndex - MAX_SLOTS_TO_REMEMBER);
    }

    // Move all the remaining to the next highest level don't move the
    // largest array.
    for (size_t n = mReceivedTransactions.size() - 1; n > 0; n--)
    {
        for (auto& tx : mReceivedTransactions[n - 1])
        {
            mReceivedTransactions[n].push_back(tx);
        }
        mReceivedTransactions[n - 1].clear();
    }

    ledgerClosed();
}

void
HerderImpl::nominatingValue(uint64 slotIndex, Value const& value,
                            std::chrono::milliseconds timeout)
{
    CLOG(DEBUG, "Herder") << "nominatingValue i:" << slotIndex
                          << " t:" << timeout.count()
                          << " v: " << getValueString(value);
    mNominationTimer.cancel();

    mNominationTimer.expires_from_now(timeout);

    mNominationTimer.async_wait(
        [this, slotIndex, value]()
        {
            assert(!mCurrentValue.empty());
            nominate(slotIndex, mCurrentValue, true);
        },
        &VirtualTimer::onFailureNoop);
}

Value
HerderImpl::combineCandidates(uint64 slotIndex,
                              std::set<Value> const& candidates)
{
    Hash h;
    StellarValue comp(h, 0, 0);

    std::set<TransactionFramePtr> aggSet;

    auto const& lcl = mLedgerManager.getLastClosedLedgerHeader();

    for (auto const& c : candidates)
    {
        StellarValue sv;
        xdr::xdr_from_opaque(c, sv);
        // max fee
        if (comp.baseFee < sv.baseFee)
        {
            comp.baseFee = sv.baseFee;
        }
        // max closeTime
        if (comp.closeTime < sv.closeTime)
        {
            comp.closeTime = sv.closeTime;
        }
        // union of all transactions
        TxSetFramePtr cTxSet = getTxSet(comp.txSetHash);
        if (cTxSet && cTxSet->previousLedgerHash() == lcl.hash)
        {
            for (auto const& tx : cTxSet->mTransactions)
            {
                aggSet.insert(tx);
            }
        }
    }
    TxSetFramePtr aggTxSet = std::make_shared<TxSetFrame>(lcl.hash);
    for (auto const& tx : aggSet)
    {
        aggTxSet->add(tx);
    }

    std::vector<TransactionFramePtr> removed;
    aggTxSet->trimInvalid(mApp, removed);
    aggTxSet->surgePricingFilter(mApp);

    comp.txSetHash = aggTxSet->getContentsHash();

    mPendingEnvelopes.recvTxSet(comp.txSetHash, aggTxSet);

    return xdr::xdr_to_opaque(comp);
}

void
HerderImpl::nodeTouched(uint256 const& nodeID)
{
    // We simply store the time of last access each time a node is touched by
    // SCP. That way we can evict old irrelevant nodes at each round.
    mNodeLastAccess[nodeID] = mApp.getClock().now();
    mNodeLastAccessSize.set_count(mNodeLastAccess.size());
}

void
HerderImpl::rebroadcast()
{
    if (mLastSentMessage.type() == SCP_MESSAGE &&
        !mApp.getConfig().MANUAL_CLOSE)
    {
        CLOG(DEBUG, "Herder")
            << "rebroadcast "
            << " s:" << mLastSentMessage.envelope().statement.pledges.type()
            << " i:" << mLastSentMessage.envelope().statement.slotIndex;

        mEnvelopeEmit.Mark();
        mApp.getOverlayManager().broadcastMessage(mLastSentMessage, true);
        startRebroadcastTimer();
    }
}

void
HerderImpl::startRebroadcastTimer()
{
    if (mLastSentMessage.type() == SCP_MESSAGE)
    {
        mRebroadcastTimer.expires_from_now(std::chrono::seconds(2));

        mRebroadcastTimer.async_wait(std::bind(&HerderImpl::rebroadcast, this),
                                     &VirtualTimer::onFailureNoop);
    }
}

void
HerderImpl::emitEnvelope(SCPEnvelope const& envelope)
{
    // this should not happen: if we're just watching consensus
    // don't send out SCP messages
    if (getSecretKey().isZero())
    {
        return;
    }

    // SCP may emit envelopes as our instance changes state
    // yet, we do not want to send those out as we don't do full validation
    // when out of sync
    if (!mTrackingSCP || !mLedgerManager.isSynced())
    {
        return;
    }

    // start to broadcast our latest message
    mLastSentMessage.type(SCP_MESSAGE);
    mLastSentMessage.envelope() = envelope;

    CLOG(DEBUG, "Herder") << "emitEnvelope"
                          << " s:" << envelope.statement.pledges.type()
                          << " i:" << envelope.statement.slotIndex
                          << " a:" << mApp.getStateHuman();

    rebroadcast();
}

bool
HerderImpl::recvTransactions(TxSetFramePtr txSet)
{
    bool allGood = true;
    for (auto tx : txSet->sortForApply())
    {
        if (recvTransaction(tx) != TX_STATUS_PENDING)
        {
            allGood = false;
        }
    }
    return allGood;
}

Herder::TransactionSubmitStatus
HerderImpl::recvTransaction(TransactionFramePtr tx)
{
    Hash const& txID = tx->getFullHash();

    // determine if we have seen this tx before and if not if it has the right
    // seq num
    int64_t totFee = tx->getFee();
    SequenceNumber highSeq = 0;

    for (auto& list : mReceivedTransactions)
    {
        for (auto oldTX : list)
        {
            if (txID == oldTX->getFullHash())
            {
                return TX_STATUS_DUPLICATE;
            }
            if (oldTX->getSourceID() == tx->getSourceID())
            {
                totFee += oldTX->getFee();
                if (oldTX->getSeqNum() > highSeq)
                {
                    highSeq = oldTX->getSeqNum();
                }
            }
        }
    }

    if (!tx->checkValid(mApp, highSeq))
    {
        return TX_STATUS_ERROR;
    }

    if (tx->getSourceAccount().getBalanceAboveReserve(mLedgerManager) < totFee)
    {
        tx->getResult().result.code(txINSUFFICIENT_BALANCE);
        return TX_STATUS_ERROR;
    }

    mReceivedTransactions[0].push_back(tx);

    return TX_STATUS_PENDING;
}

void
HerderImpl::recvSCPEnvelope(SCPEnvelope const& envelope)
{
    if (mApp.getConfig().MANUAL_CLOSE)
    {
        return;
    }

    CLOG(DEBUG, "Herder") << "recvSCPEnvelope"
                          << " from: " << hexAbbrev(envelope.statement.nodeID)
                          << " s:" << envelope.statement.pledges.type()
                          << " i:" << envelope.statement.slotIndex
                          << " a:" << mApp.getStateHuman();

    mEnvelopeReceive.Mark();

    if (mTrackingSCP)
    {
        // when tracking, we can filter messages based on the information we got
        // from consensus
        uint32_t minLedgerSeq = nextConsensusLedgerIndex();
        uint32_t maxLedgerSeq =
            nextConsensusLedgerIndex() + LEDGER_VALIDITY_BRACKET;

        // If we are fully synced and the envelopes are out of our validity
        // brackets, we just ignore them.
        if (envelope.statement.slotIndex > maxLedgerSeq ||
            envelope.statement.slotIndex < minLedgerSeq)
        {
            CLOG(DEBUG, "Herder") << "Ignoring SCPEnvelope outside of range: "
                                  << envelope.statement.slotIndex << "( "
                                  << minLedgerSeq << "," << maxLedgerSeq << ")";
            return;
        }
    }

    mPendingEnvelopes.recvSCPEnvelope(envelope);
}

void
HerderImpl::processSCPQueue()
{
    if (mTrackingSCP)
    {
        // drop obsolete slots
        mPendingEnvelopes.eraseBelow(nextConsensusLedgerIndex());

        // process current slot only
        processSCPQueueAtIndex(nextConsensusLedgerIndex());
    }
    else
    {
        // we don't know which ledger we're in
        // try to consume the messages from the queue
        // starting from the smallest slot
        for (auto& slot : mPendingEnvelopes.readySlots())
        {
            processSCPQueueAtIndex(slot);
            if (mTrackingSCP)
            {
                // one of the slots externalized
                // we go back to regular flow
                break;
            }
        }
    }
}

void
HerderImpl::processSCPQueueAtIndex(uint64 slotIndex)
{
    while (true)
    {
        SCPEnvelope env;
        if (mPendingEnvelopes.pop(slotIndex, env))
        {
            receiveEnvelope(env);
        }
        else
        {
            return;
        }
    }
}

void
HerderImpl::ledgerClosed()
{
    mTriggerTimer.cancel();

    updateSCPCounters();
    CLOG(TRACE, "Herder") << "HerderImpl::ledgerClosed";

    mPendingEnvelopes.slotClosed(lastConsensusLedgerIndex());

    mApp.getOverlayManager().ledgerClosed(lastConsensusLedgerIndex());

    // As the current slotIndex changes we cancel all pending validation
    // timers. Since the value externalized, the messages that this generates
    // wont' have any impact.
    mBallotValidationTimers.clear();
    mBallotValidationTimersSize.set_count(mBallotValidationTimers.size());

    uint64_t nextIndex = nextConsensusLedgerIndex();

    // process any statements for this slot (this may trigger externalize)
    processSCPQueueAtIndex(nextIndex);

    // if externalize got called for a future slot, we don't
    // need to trigger (the now obsolete) next round
    if (nextIndex != nextConsensusLedgerIndex())
    {
        return;
    }

    // If we are not a validating node and just watching SCP we don't call
    // triggerNextLedger. Likewise if we are not in synced state.
    if (getSecretKey().isZero())
    {
        CLOG(DEBUG, "Herder")
            << "Non-validating node, not triggering ledger-close.";
        return;
    }

    if (!mLedgerManager.isSynced())
    {
        CLOG(DEBUG, "Herder")
            << "Not presently synced, not triggering ledger-close.";
        return;
    }

    auto seconds = Herder::EXP_LEDGER_TIMESPAN_SECONDS;
    if (mApp.getConfig().ARTIFICIALLY_ACCELERATE_TIME_FOR_TESTING)
    {
        seconds = std::chrono::seconds(1);
    }

    auto now = mApp.getClock().now();
    if ((now - mLastTrigger) < seconds)
    {
        auto timeout = seconds - (now - mLastTrigger);
        mTriggerTimer.expires_from_now(timeout);
    }
    else
    {
        mTriggerTimer.expires_from_now(std::chrono::nanoseconds(0));
    }

    if (!mApp.getConfig().MANUAL_CLOSE)
        mTriggerTimer.async_wait(std::bind(&HerderImpl::triggerNextLedger, this,
                                           static_cast<uint32_t>(nextIndex)),
                                 &VirtualTimer::onFailureNoop);
}

void
HerderImpl::removeReceivedTx(TransactionFramePtr dropTx)
{
    for (auto& list : mReceivedTransactions)
    {
        for (auto iter = list.begin(); iter != list.end();)
        {
            if ((iter.operator->())->get()->getFullHash() ==
                dropTx->getFullHash())
            {
                list.erase(iter);
                return;
            }
            else
            {
                ++iter;
            }
        }
    }
}

void
HerderImpl::recvSCPQuorumSet(Hash hash, const SCPQuorumSet& qset)
{
    mPendingEnvelopes.recvSCPQuorumSet(hash, qset);
}

void
HerderImpl::recvTxSet(Hash hash, const TxSetFrame& t)
{
    TxSetFramePtr txset(new TxSetFrame(t));
    mPendingEnvelopes.recvTxSet(hash, txset);
}

void
HerderImpl::peerDoesntHave(MessageType type, uint256 const& itemID,
                           PeerPtr peer)
{
    mPendingEnvelopes.peerDoesntHave(type, itemID, peer);
}

TxSetFramePtr
HerderImpl::getTxSet(Hash hash)
{
    return mPendingEnvelopes.getTxSet(hash);
}

SCPQuorumSetPtr
HerderImpl::getQSet(const Hash& qSetHash)
{
    return mPendingEnvelopes.getQSet(qSetHash);
}

uint32_t
HerderImpl::getCurrentLedgerSeq() const
{
    if (mTrackingSCP)
    {
        return static_cast<uint32_t>(mTrackingSCP->mConsensusIndex);
    }
    else
    {
        return mLedgerManager.getLastClosedLedgerNum();
    }
}

// called to take a position during the next round
// uses the state in LedgerManager to derive a starting position
void
HerderImpl::triggerNextLedger(uint32_t ledgerSeqToTrigger)
{
    if (!mTrackingSCP || !mLedgerManager.isSynced())
    {
        CLOG(DEBUG, "Herder") << "triggerNextLedger: skipping (out of sync) : "
                              << mApp.getStateHuman();
        return;
    }
    updateSCPCounters();

    // our first choice for this round's set is all the tx we have collected
    // during last ledger close
    auto const& lcl = mLedgerManager.getLastClosedLedgerHeader();
    TxSetFramePtr proposedSet = std::make_shared<TxSetFrame>(lcl.hash);

    for (auto& list : mReceivedTransactions)
    {
        for (auto& tx : list)
        {
            proposedSet->add(tx);
        }
    }

    std::vector<TransactionFramePtr> removed;
    proposedSet->trimInvalid(mApp, removed);
    for (auto& tx : removed)
    {
        removeReceivedTx(tx);
    }

    proposedSet->surgePricingFilter(mApp);

    auto txSetHash = proposedSet->getContentsHash();

    // add all txs to next set in case they don't get in this ledger
    recvTransactions(proposedSet);

    // Inform the item fetcher so queries from other peers about his txSet
    // can be answered. Note this can trigger SCP callbacks, externalize, etc
    // if we happen to build a txset that we were trying to download.
    mPendingEnvelopes.recvTxSet(txSetHash, proposedSet);

    // use the slot index from ledger manager here as our vote is based off
    // the last closed ledger stored in ledger manager
    uint32_t slotIndex = lcl.header.ledgerSeq + 1;

    // no point in sending out a prepare:
    // externalize was triggered on a more recent ledger
    if (ledgerSeqToTrigger != slotIndex)
    {
        return;
    }

    // We store at which time we triggered consensus
    mLastTrigger = mApp.getClock().now();

    // We pick as next close time the current time unless it's before the last
    // close time. We don't know how much time it will take to reach consensus
    // so this is the most appropriate value to use as closeTime.
    uint64_t nextCloseTime = VirtualClock::to_time_t(mLastTrigger);
    if (nextCloseTime <= lcl.header.closeTime)
    {
        nextCloseTime = lcl.header.closeTime + 1;
    }

    StellarValue b;
    b.txSetHash = txSetHash;
    b.closeTime = nextCloseTime;
    b.baseFee = mApp.getConfig().DESIRED_BASE_FEE;

    mCurrentValue = xdr::xdr_to_opaque(b);

    uint256 valueHash = sha256(xdr::xdr_to_opaque(mCurrentValue));
    CLOG(DEBUG, "Herder") << "HerderImpl::triggerNextLedger"
                          << " txSet.size: "
                          << proposedSet->mTransactions.size()
                          << " previousLedgerHash: "
                          << hexAbbrev(proposedSet->previousLedgerHash())
                          << " value: " << hexAbbrev(valueHash)
                          << " slot: " << slotIndex;

    mValuePrepare.Mark();
    nominate(slotIndex, mCurrentValue, false);
}

void
HerderImpl::expireBallot(uint64 slotIndex, SCPBallot const& ballot)

{
    mBallotExpire.Mark();
    assert(slotIndex == nextConsensusLedgerIndex());

    mValuePrepare.Mark();
    abandonBallot(slotIndex);
}

// Extra SCP methods overridden solely to increment metrics.
void
HerderImpl::ballotDidPrepare(uint64 slotIndex, SCPBallot const& ballot)
{
    mBallotPrepare.Mark();
}

void
HerderImpl::ballotDidPrepared(uint64 slotIndex, SCPBallot const& ballot)
{
    mBallotPrepared.Mark();
}

void
HerderImpl::ballotDidCommit(uint64 slotIndex, SCPBallot const& ballot)
{
    mBallotCommit.Mark();
}

void
HerderImpl::ballotDidCommitted(uint64 slotIndex, SCPBallot const& ballot)
{
    mBallotCommitted.Mark();
}

void
HerderImpl::envelopeSigned()
{
    mEnvelopeSign.Mark();
}

void
HerderImpl::envelopeVerified(bool valid)
{
    if (valid)
    {
        mEnvelopeValidSig.Mark();
    }
    else
    {
        mEnvelopeInvalidSig.Mark();
    }
}

void
HerderImpl::dumpInfo(Json::Value& ret)
{
    int count = 0;
    for (auto& item : mKnownNodes)
    {
        ret["nodes"][count++] =
            toBase58Check(VER_ACCOUNT_ID, item.second->getNodeID()).c_str();
    }

    ret["you"] = hexAbbrev(getSecretKey().getPublicKey());

    for (auto& item : mKnownSlots)
    {
        item.second->dumpInfo(ret);
    }

    mPendingEnvelopes.dumpInfo(ret);
}

void
HerderImpl::trackingHeartBeat()
{
    if (mApp.getConfig().MANUAL_CLOSE)
    {
        return;
    }

    assert(mTrackingSCP);
    mTrackingTimer.expires_from_now(
        std::chrono::seconds(CONSENSUS_STUCK_TIMEOUT_SECONDS));
    mTrackingTimer.async_wait(std::bind(&HerderImpl::herderOutOfSync, this),
                              &VirtualTimer::onFailureNoop);
}

void
HerderImpl::herderOutOfSync()
{
    CLOG(INFO, "Herder") << "Lost track of consensus";
    mLostSync.Mark();
    mTrackingSCP.reset();
    processSCPQueue();
}
}
