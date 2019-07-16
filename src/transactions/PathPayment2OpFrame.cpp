// Copyright 2014 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "util/asio.h"
#include "transactions/PathPayment2OpFrame.h"
#include "OfferExchange.h"
#include "database/Database.h"
#include "ledger/LedgerTxn.h"
#include "ledger/LedgerTxnEntry.h"
#include "ledger/LedgerTxnHeader.h"
#include "ledger/TrustLineWrapper.h"
#include "transactions/TransactionUtils.h"
#include "util/Logging.h"
#include "util/XDROperators.h"
#include <algorithm>

#include "main/Application.h"

/*
There are 3 ways to handle dust that can't be sent through the offer:
1) sender keeps
2) try to give to offers in the first hop
3) burn 

We are using 3) for assets and 2) for XLM

*/

namespace stellar
{

using namespace std;

PathPayment2OpFrame::PathPayment2OpFrame(Operation const& op,
                                         OperationResult& res,
                                         TransactionFrame& parentTx)
    : OperationFrame(op, res, parentTx)
    , mPathPayment2(mOperation.body.pathPayment2Op())
{
}

bool
PathPayment2OpFrame::doApply(AbstractLedgerTxn& ltx)
{
    innerResult().code(PATH_PAYMENT_SUCCESS);

    // tracks the current asset and amount to send to the next step
    Asset curA = mPathPayment2.sendAsset;
    int64_t curAAmount = mPathPayment2.sendAmount;

    if (mPathPayment2.sendAsset.type() == ASSET_TYPE_NATIVE)
    {
        auto header = ltx.loadHeader();
        LedgerTxnEntry sourceAccount;

        sourceAccount = stellar::loadAccount(ltx, getSourceID());
        if (!sourceAccount) // TODO NICO: is this possible? isn't this checked at the
                            // tx envelope? why only checking for native assets
        {
            innerResult().code(PATH_PAYMENT_MALFORMED);
            return false;
        }

        if (mPathPayment2.sendAmount >
            getAvailableBalance(header, sourceAccount))
        { // they don't have enough to send
            innerResult().code(PATH_PAYMENT_UNDERFUNDED);
            return false;
        }

        auto ok = addBalance(header, sourceAccount, -mPathPayment2.sendAmount);
        assert(ok);
    }
    else
    {
        auto sourceLine =
            loadTrustLine(ltx, getSourceID(), mPathPayment2.sendAsset);
        if (!sourceLine)
        {
            innerResult().code(PATH_PAYMENT_SRC_NO_TRUST);
            return false;
        }

        if (!sourceLine.isAuthorized())
        {
            innerResult().code(PATH_PAYMENT_SRC_NOT_AUTHORIZED);
            return false;
        }

        if (!sourceLine.addBalance(ltx.loadHeader(), -mPathPayment2.sendAmount))
        {
            innerResult().code(PATH_PAYMENT_UNDERFUNDED);
            return false;
        }
    }

    // build the full path to the destination, starting with sendAsset
    std::vector<Asset> fullPath;
    fullPath.emplace_back(curA);
    fullPath.insert(fullPath.end(), mPathPayment2.path.begin(),
                    mPathPayment2.path.end());
    fullPath.emplace_back(mPathPayment2.destAsset);

    // if the payment doesn't involve intermediate accounts
    // and the destination is the issuer we don't bother
    // checking if the destination account even exist
    // so that it's always possible to send credits back to its issuer
    bool bypassIssuerCheck =
        (curA.type() != ASSET_TYPE_NATIVE) && (fullPath.size() == 2) &&
        (mPathPayment2.sendAsset == mPathPayment2.destAsset) &&
        (getIssuer(curA) == mPathPayment2.destination);

    if (!bypassIssuerCheck)
    {
        if (!stellar::loadAccountWithoutRecord(ltx, mPathPayment2.destination))
        {
            innerResult().code(PATH_PAYMENT_NO_DESTINATION);
            return false;
        }

        if (mPathPayment2.destAsset.type() != ASSET_TYPE_NATIVE)
        {
            auto issuer = stellar::loadAccountWithoutRecord(
                ltx, getIssuer(mPathPayment2.destAsset));
            if (!issuer)
            {
                innerResult().code(PATH_PAYMENT_NO_ISSUER);
                innerResult().noIssuer() = mPathPayment2.destAsset;
                return false;
            }
        }

        if (curA.type() != ASSET_TYPE_NATIVE)
        {
            if (!stellar::loadAccountWithoutRecord(ltx, getIssuer(curA)))
            {
                innerResult().code(PATH_PAYMENT_NO_ISSUER);
                innerResult().noIssuer() = curA;
                return false;
            }
        }
    }

    // now, walk the path
    for (int i = 1; i < fullPath.size(); i++)
    {
        int64_t actualCurASent, actualCurBReceived;
        Asset const& curB = fullPath[i];

        if (curA == curB)
        {
            continue;
        }

        if (curB.type() != ASSET_TYPE_NATIVE)
        {
            if (!stellar::loadAccountWithoutRecord(ltx, getIssuer(curB)))
            {
                innerResult().code(PATH_PAYMENT_NO_ISSUER);
                innerResult().noIssuer() = curB;
                return false;
            }
        }

        size_t offersCrossedSoFar = innerResult().success().offers.size();
        // offersCrossed will never be bigger than INT64_MAX because
        // - the machine would have run out of memory
        // - the limit, which cannot exceed INT64_MAX, should be enforced
        // so this subtraction is safe because MAX_OFFERS_TO_CROSS >= 0
        int64_t maxOffersToCross = MAX_OFFERS_TO_CROSS - offersCrossedSoFar;

        // curA -> curB
        std::vector<ClaimOfferAtom> offerTrail;
        ConvertResult r = convertWithOffers(
            ltx, curA, curAAmount, actualCurASent, curB, INT64_MAX,
            actualCurBReceived, true,
            [this](LedgerTxnEntry const& o) {
                auto const& offer = o.current().data.offer();
                if (offer.sellerID == getSourceID())
                {
                    // we are crossing our own offer
                    innerResult().code(PATH_PAYMENT_OFFER_CROSS_SELF);
                    return OfferFilterResult::eStop;
                }
                return OfferFilterResult::eKeep;
            },
            offerTrail, maxOffersToCross);

        switch (r)
        {
        case ConvertResult::eFilterStop:
            return false;
        // fall through
        case ConvertResult::ePartial:
            innerResult().code(PATH_PAYMENT_TOO_FEW_OFFERS);
            return false;
        case ConvertResult::eCrossedTooMany:
            mResult.code(opEXCEEDED_WORK_LIMIT);
            return false;
        }

        assert(curAAmount >= actualCurASent);
        
        /* TODOJED: this shouldn't have to happen here if it is moved into convertWithOffers
        if ( curAAmount != actualCurASent &&
             curA.type() == ASSET_TYPE_NATIVE)
        {
            AccountID winner;
            if (offerTrail.size())
            {
                winner = offerTrail[0].sellerID;
            }
            else
            {
                winner = 
            }

            auto ok = addBalance(header, ,
                                 actualCurASent - curAAmount);
            assert(ok);
        }
*/       

        // add offers that got taken on the way
        // insert in front to match the path's order
        auto& offers = innerResult().success().offers;
        offers.insert(offers.begin(), offerTrail.begin(), offerTrail.end());

        // set up next round
        curAAmount = actualCurBReceived;
        curA = curB;
    }
    // end path loop curAAmount is now the amount recieved

    if (curAAmount < mPathPayment2.destMinAmount)
    { // make sure over the min
        innerResult().code(PATH_PAYMENT_OVER_SENDMAX);
        return false;
    }

    if (curA.type() == ASSET_TYPE_NATIVE)
    {
        auto destination = stellar::loadAccount(ltx, mPathPayment2.destination);
        if (!addBalance(ltx.loadHeader(), destination, curAAmount))
        {
            innerResult().code(PATH_PAYMENT_LINE_FULL);

            return false;
        }
    }
    else
    {
        auto destLine =
            stellar::loadTrustLine(ltx, mPathPayment2.destination, curA);
        if (!destLine)
        {
            innerResult().code(PATH_PAYMENT_NO_TRUST);
            return false;
        }

        if (!destLine.isAuthorized())
        {
            innerResult().code(PATH_PAYMENT_NOT_AUTHORIZED);
            return false;
        }

        if (!destLine.addBalance(ltx.loadHeader(), curAAmount))
        {
            innerResult().code(PATH_PAYMENT_LINE_FULL);
            return false;
        }
    }

    innerResult().success().last =
        SimplePaymentResult(mPathPayment2.destination, curA, curAAmount);

    return true;
}

bool
PathPayment2OpFrame::doCheckValid(uint32_t ledgerVersion)
{
    if (mPathPayment2.sendAmount <= 0 || mPathPayment2.destMinAmount < 0)
    {
        innerResult().code(PATH_PAYMENT_MALFORMED);
        return false;
    }
    if (!isAssetValid(mPathPayment2.sendAsset) ||
        !isAssetValid(mPathPayment2.destAsset))
    {
        innerResult().code(PATH_PAYMENT_MALFORMED);
        return false;
    }
    auto const& p = mPathPayment2.path;
    if (!std::all_of(p.begin(), p.end(), isAssetValid))
    {
        innerResult().code(PATH_PAYMENT_MALFORMED);
        return false;
    }
    return true;
}

void
PathPayment2OpFrame::insertLedgerKeysToPrefetch(
    std::unordered_set<LedgerKey>& keys) const
{
    keys.emplace(accountKey(mPathPayment2.destination));

    auto processAsset = [&](Asset const& asset) {
        if (asset.type() != ASSET_TYPE_NATIVE)
        {
            auto issuer = getIssuer(asset);
            keys.emplace(accountKey(issuer));
        }
    };

    processAsset(mPathPayment2.sendAsset);
    processAsset(mPathPayment2.destAsset);
    std::for_each(mPathPayment2.path.begin(), mPathPayment2.path.end(),
                  processAsset);

    if (mPathPayment2.destAsset.type() != ASSET_TYPE_NATIVE)
    {
        keys.emplace(
            trustlineKey(mPathPayment2.destination, mPathPayment2.destAsset));
    }
    if (mPathPayment2.sendAsset.type() != ASSET_TYPE_NATIVE)
    {
        keys.emplace(trustlineKey(getSourceID(), mPathPayment2.sendAsset));
    }
}
}
