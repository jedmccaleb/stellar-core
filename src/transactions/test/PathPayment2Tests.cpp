// Copyright 2017 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "ledger/LedgerTxn.h"
#include "ledger/LedgerTxnEntry.h"
#include "ledger/LedgerTxnHeader.h"
#include "lib/catch.hpp"
#include "test/TestAccount.h"
#include "test/TestExceptions.h"
#include "test/TestMarket.h"
#include "test/TestUtils.h"
#include "test/TxTests.h"
#include "test/test.h"
#include "transactions/TransactionUtils.h"
#include "util/Timer.h"

#include <deque>
#include <limits>

using namespace stellar;
using namespace stellar::txtest;

// market.addOffer(mm, {selling, buying, Price{buy, sell}, amountSelling});

namespace
{

int64_t operator*(int64_t x, const Price& y)
{
    bool xNegative = (x < 0);
    int64_t m = bigDivide(xNegative ? -x : x, y.n, y.d, Rounding::ROUND_DOWN);
    return xNegative ? -m : m;
}

Price operator*(const Price& x, const Price& y)
{
    int64_t n = int64_t(x.n) * int64_t(y.n);
    int64_t d = int64_t(x.d) * int64_t(y.d);
    assert(n <= std::numeric_limits<int32_t>::max());
    assert(n >= 0);
    assert(d <= std::numeric_limits<int32_t>::max());
    assert(d >= 1);
    return Price{(int32_t)n, (int32_t)d};
}

template <typename T>
void
rotateRight(std::deque<T>& d)
{
    auto e = d.back();
    d.pop_back();
    d.push_front(e);
}

std::string
assetToString(const Asset& asset)
{
    auto r = std::string{};
    switch (asset.type())
    {
    case stellar::ASSET_TYPE_NATIVE:
        r = std::string{"XLM"};
        break;
    case stellar::ASSET_TYPE_CREDIT_ALPHANUM4:
        assetCodeToStr(asset.alphaNum4().assetCode, r);
        break;
    case stellar::ASSET_TYPE_CREDIT_ALPHANUM12:
        assetCodeToStr(asset.alphaNum12().assetCode, r);
        break;
    }
    return r;
};

std::string
assetPathToString(const std::deque<Asset>& assets)
{
    auto r = assetToString(assets[0]);
    for (auto i = assets.rbegin(); i != assets.rend(); i++)
    {
        r += " -> " + assetToString(*i);
    }
    return r;
};
}

TEST_CASE("pathpayment2", "[tx][pathpayment]")
{
    auto const& cfg = getTestConfig();

    VirtualClock clock;
    auto app = createTestApplication(clock, cfg);
    app->start();

    // set up world
    auto root = TestAccount::createRoot(*app);
    auto xlm = makeNativeAsset();
    auto txfee = app->getLedgerManager().getLastTxFee();

    auto const minBalanceNoTx = app->getLedgerManager().getLastMinBalance(0);
    auto const minBalance =
        app->getLedgerManager().getLastMinBalance(0) + 10 * txfee;

    auto const minBalance1 =
        app->getLedgerManager().getLastMinBalance(1) + 10 * txfee;
    auto const minBalance2 =
        app->getLedgerManager().getLastMinBalance(2) + 10 * txfee;
    auto const minBalance3 =
        app->getLedgerManager().getLastMinBalance(3) + 10 * txfee;
    auto const minBalance4 =
        app->getLedgerManager().getLastMinBalance(4) + 10 * txfee;
    auto const minBalance5 =
        app->getLedgerManager().getLastMinBalance(5) + 10 * txfee;

    auto const paymentAmount = minBalance3;
    auto const morePayment = paymentAmount / 2;
    auto const trustLineLimit = INT64_MAX;

    // sets up gateway account
    auto const gatewayPayment = minBalance2 + morePayment;
    auto gateway = root.create("gate", gatewayPayment);

    // sets up gateway2 account
    auto gateway2 = root.create("gate2", gatewayPayment);

    auto idr = makeAsset(gateway, "IDR");
    auto cur1 = makeAsset(gateway, "CUR1");
    auto cur2 = makeAsset(gateway, "CUR2");
    auto usd = makeAsset(gateway2, "USD");
    auto cur3 = makeAsset(gateway2, "CUR3");
    auto cur4 = makeAsset(gateway2, "CUR4");

    closeLedgerOn(*app, 2, 1, 1, 2016);
    
        SECTION("path payment destination amount negative")
        {
            auto market = TestMarket{*app};
            auto source = root.create("source", minBalance1);
            auto destination = root.create("destination", minBalance);
            source.changeTrust(idr, 20);
            gateway.pay(source, idr, 10);
            for_all_versions(*app, [&] {
                REQUIRE_THROWS_AS(source.pathpay2(destination, idr, 10, idr, -1,
     {}), ex_PATH_PAYMENT_MALFORMED);
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance1 - 2 * txfee}, {idr, 10}, {usd,
     0}}}, {destination, {{xlm, minBalance}, {idr, 0}, {usd, 0}}}});
                // clang-format on
            });
        }

        SECTION("path payment send amount negative")
        {
            auto market = TestMarket{*app};
            auto source = root.create("source", minBalance1);
            auto destination = root.create("destination", minBalance);
            source.changeTrust(idr, 20);
            gateway.pay(source, idr, 10);
            for_all_versions(*app, [&] {
                REQUIRE_THROWS_AS(
                    source.pathpay2(destination, idr, -1, idr, 10, {}),
                                  ex_PATH_PAYMENT_MALFORMED);
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance1 - 2 * txfee}, {idr, 10}, {usd,
     0}}}, {destination, {{xlm, minBalance}, {idr, 0}, {usd, 0}}}});
                // clang-format on
            });
        }

        SECTION("path payment destination min 0")
        {
            auto market = TestMarket{*app};
            auto source = root.create("source", minBalance1);
            auto destination = root.create("destination", minBalance1);
            destination.changeTrust(idr, 20);
            source.changeTrust(idr, 20);
            gateway.pay(source, idr, 10);
            for_all_versions(*app, [&] {
                source.pathpay2(destination, idr, 10, idr, 0, {}),
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance1 - 2 * txfee}, {idr, 0}, {usd,
     0}}}, {destination, {{xlm, minBalance1 - 1 * txfee}, {idr, 10}, {usd,
     0}}}});
                // clang-format on
            });
        }

        SECTION("path payment send currency invalid")
        {
            auto market = TestMarket{*app};
            auto source = root.create("source", minBalance1);
            auto destination = root.create("destination", minBalance);
            source.changeTrust(idr, 20);
            gateway.pay(source, idr, 10);
            for_all_versions(*app, [&] {
                REQUIRE_THROWS_AS(
                    source.pathpay2(destination, makeInvalidAsset(), 10, idr,
     10, {}), ex_PATH_PAYMENT_MALFORMED);
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance1 - 2 * txfee}, {idr, 10}, {usd,
     0}}}, {destination, {{xlm, minBalance}, {idr, 0}, {usd, 0}}}});
                // clang-format on
            });
        }

        SECTION("path payment destination currency invalid")
        {
            auto market = TestMarket{*app};
            auto source = root.create("source", minBalance1);
            auto destination = root.create("destination", minBalance);
            source.changeTrust(idr, 20);
            gateway.pay(source, idr, 10);
            for_all_versions(*app, [&] {
                REQUIRE_THROWS_AS(source.pathpay2(destination, idr, 10,
                                                  makeInvalidAsset(), 10, {}),
                    ex_PATH_PAYMENT_MALFORMED);
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance1 - 2 * txfee}, {idr, 10}, {usd,
     0}}}, {destination, {{xlm, minBalance}, {idr, 0}, {usd, 0}}}});
                // clang-format on
            });
        }

        SECTION("path payment destination path currency invalid")
        {
            auto market = TestMarket{*app};
            auto source = root.create("source", minBalance1);
            auto destination = root.create("destination", minBalance);
            source.changeTrust(idr, 20);
            gateway.pay(source, idr, 10);
            for_all_versions(*app, [&] {
                REQUIRE_THROWS_AS(source.pathpay2(destination, idr, 10, idr, 10,
                                                  {makeInvalidAsset()}),
                    ex_PATH_PAYMENT_MALFORMED);
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance1 - 2 * txfee}, {idr, 10}, {usd,
     0}}}, {destination, {{xlm, minBalance}, {idr, 0}, {usd, 0}}}});
                // clang-format on
            });
        }
        


        SECTION("path payment XLM with not enough funds")
        {
            auto market = TestMarket{*app};
            // see https://github.com/stellar/stellar-core/pull/1239
            auto minimumAccount =
                root.create("minimum-account", minBalanceNoTx + 2 * txfee + 20);
            for_all_versions(*app, [&] {
                REQUIRE_THROWS_AS(
                    minimumAccount.pathpay2(root, xlm, txfee + 21, xlm, 0, {}),
                    ex_PATH_PAYMENT_UNDERFUNDED);
                // clang-format off
                market.requireBalances(
                    {{minimumAccount, {{xlm, minBalanceNoTx + txfee + 20}, {idr,
     0}, {usd, 0}}}});
                // clang-format on
            });
        }

        SECTION("path payment asset with not enough funds")
        {
            auto market = TestMarket{*app};
            auto minimumAccount = root.create("minimum-account", minBalance1);
            auto destination = root.create("destination", minBalance1);
            minimumAccount.changeTrust(idr, 20);
            destination.changeTrust(idr, 20);
            gateway.pay(minimumAccount, idr, 10);
            for_all_versions(*app, [&] {
                REQUIRE_THROWS_AS(
                    minimumAccount.pathpay2(gateway, idr, 11, idr, 11, {}),
                                  ex_PATH_PAYMENT_UNDERFUNDED);
                // clang-format off
                market.requireBalances(
                    {{minimumAccount, {{xlm, minBalance1 - 2 * txfee}, {idr,
     10}, {usd, 0}}}, {destination, {{xlm, minBalance1 - txfee}, {idr, 0}, {usd,
     0}}}});
                // clang-format on
                REQUIRE_THROWS_AS(
                    minimumAccount.pathpay2(destination, idr, 11, idr, 11, {}),
                    ex_PATH_PAYMENT_UNDERFUNDED);
                // clang-format off
                market.requireBalances(
                    {{minimumAccount, {{xlm, minBalance1 - 3 * txfee}, {idr,
     10}, {usd, 0}}}, {destination, {{xlm, minBalance1 - txfee}, {idr, 0}, {usd,
     0}}}});
                // clang-format on
            });
        }

        SECTION("path payment source does not have trustline")
        {
            auto market = TestMarket{*app};
            auto noSourceTrust = root.create("no-source-trust", minBalance);
            auto destination = root.create("destination", minBalance1);
            destination.changeTrust(idr, 20);
            for_all_versions(*app, [&] {
                REQUIRE_THROWS_AS(
                    noSourceTrust.pathpay2(gateway, idr, 1, idr, 1, {}),
                                  ex_PATH_PAYMENT_SRC_NO_TRUST);
                // clang-format off
                market.requireBalances(
                    {{noSourceTrust, {{xlm, minBalance - txfee}, {idr, 0}, {usd,
     0}}}, {destination, {{xlm, minBalance1 - txfee}, {idr, 0}, {usd, 0}}}});
                // clang-format on
                REQUIRE_THROWS_AS(
                    noSourceTrust.pathpay2(destination, idr, 1, idr, 1, {}),
                    ex_PATH_PAYMENT_SRC_NO_TRUST);
                // clang-format off
                market.requireBalances(
                    {{noSourceTrust, {{xlm, minBalance - 2 * txfee}, {idr, 0},
     {usd, 0}}}, {destination, {{xlm, minBalance1 - txfee}, {idr, 0}, {usd,
     0}}}});
                // clang-format on
            });
        }
        
        SECTION("path payment source is not authorized")
        {
            auto market = TestMarket{*app};
            auto noAuthorizedSourceTrust =
                root.create("no-authorized-source-trust", minBalance1);
            auto destination = root.create("destination", minBalance1);
            noAuthorizedSourceTrust.changeTrust(idr, 20);
            gateway.pay(noAuthorizedSourceTrust, idr, 10);
            destination.changeTrust(idr, 20);
            gateway.setOptions(
                setFlags(uint32_t{AUTH_REQUIRED_FLAG | AUTH_REVOCABLE_FLAG}));
            gateway.denyTrust(idr, noAuthorizedSourceTrust);
            for_all_versions(*app, [&] {
                REQUIRE_THROWS_AS(
                    noAuthorizedSourceTrust.pathpay2(gateway, idr, 10, idr, 10,
     {}), ex_PATH_PAYMENT_SRC_NOT_AUTHORIZED);
                // clang-format off
                market.requireBalances(
                    {{noAuthorizedSourceTrust, {{xlm, minBalance1 - 2 * txfee},
     {idr, 10}, {usd, 0}}}, {destination, {{xlm, minBalance1 - txfee}, {idr, 0},
     {usd, 0}}}});
                // clang-format on
                REQUIRE_THROWS_AS(noAuthorizedSourceTrust.pathpay2(destination,
     idr, 10, idr, 10, {}), ex_PATH_PAYMENT_SRC_NOT_AUTHORIZED);
                // clang-format off
                market.requireBalances(
                    {{noAuthorizedSourceTrust, {{xlm, minBalance1 - 3 * txfee},
     {idr, 10}, {usd, 0}}}, {destination, {{xlm, minBalance1 - txfee}, {idr, 0},
     {usd, 0}}}});
                // clang-format on
            });
        }
        
        SECTION("path payment destination does not exists")
        {
            auto market = TestMarket{*app};
            auto source = root.create("source", minBalance1);
            source.changeTrust(idr, 20);
            gateway.pay(source, idr, 10);
            for_all_versions(*app, [&] {
                REQUIRE_THROWS_AS(
                    source.pathpay2(
                        getAccount("non-existing-destination").getPublicKey(),
     idr, 10, idr, 10, {}), ex_PATH_PAYMENT_NO_DESTINATION);
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance1 - 2 * txfee}, {idr, 10}, {usd, 0}}}});
                // clang-format on
            });
        }
      
        SECTION("path payment destination is issuer and does not exists for simple "
            "paths")
        {
            auto market = TestMarket{*app};
            auto source = root.create("source", minBalance1);
            source.changeTrust(idr, 20);
            gateway.pay(source, idr, 10);
            for_all_versions(*app, [&] {
                gateway.merge(root);
                auto offers = source.pathpay2(gateway, idr, 10, idr, 0, {});
                auto expected = std::vector<ClaimOfferAtom>{};
                REQUIRE(offers.success().offers == expected);
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance1 - 2 * txfee}, {idr, 0}, {usd,
     0}}}});
                // clang-format on
            });
        }
        
        SECTION("path payment destination is issuer and does not exists for "
                "complex paths")
        {
            auto market = TestMarket{*app};
            auto source = root.create("source", minBalance1);
            source.changeTrust(idr, 20);
            gateway.pay(source, idr, 10);
            for_all_versions(*app, [&] {
                gateway.merge(root);
                REQUIRE_THROWS_AS(source.pathpay2(gateway, idr, 10, usd, 10,
     {}), ex_PATH_PAYMENT_NO_DESTINATION);
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance1 - 2 * txfee}, {idr, 10}, {usd,
     0}}}});
                // clang-format on
            });
        }
       

        

        SECTION("path payment destination does not have trustline")
        {
            auto market = TestMarket{*app};
            auto source = root.create("source", minBalance1);
            auto noDestinationTrust =
                root.create("no-destination-trust", minBalance);
            source.changeTrust(idr, 20);
            gateway.pay(source, idr, 10);
            for_all_versions(*app, [&] {
                REQUIRE_THROWS_AS(
                    gateway.pathpay2(noDestinationTrust, idr, 1, idr, 1, {}),
                    ex_PATH_PAYMENT_NO_TRUST);
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance1 - txfee}, {idr, 10}, {usd,
     0}}}, {noDestinationTrust, {{xlm, minBalance}, {idr, 0}, {usd, 0}}}});
                // clang-format on
                REQUIRE_THROWS_AS(
                    source.pathpay2(noDestinationTrust, idr, 1, idr, 1, {}),
                    ex_PATH_PAYMENT_NO_TRUST);
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance1 - 2 * txfee}, {idr, 10}, {usd,
     0}}}, {noDestinationTrust, {{xlm, minBalance}, {idr, 0}, {usd, 0}}}});
                // clang-format on
            });
        }

        SECTION("path payment destination is not authorized")
        {
            auto market = TestMarket{*app};
            auto source = root.create("source", minBalance1);
            auto noAuthorizedDestinationTrust =
                root.create("no-authorized-destination-trust", minBalance1);
            source.changeTrust(idr, 20);
            gateway.pay(source, idr, 10);
            noAuthorizedDestinationTrust.changeTrust(idr, 20);
            gateway.setOptions(
                setFlags(uint32_t{AUTH_REQUIRED_FLAG | AUTH_REVOCABLE_FLAG}));
            gateway.denyTrust(idr, noAuthorizedDestinationTrust);
            for_all_versions(*app, [&] {
                REQUIRE_THROWS_AS(gateway.pathpay2(noAuthorizedDestinationTrust,
                                                   idr, 10, idr, 10, {}),
                    ex_PATH_PAYMENT_NOT_AUTHORIZED);
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance1 - txfee}, {idr, 10}, {usd,
     0}}}, {noAuthorizedDestinationTrust, {{xlm, minBalance1 - txfee}, {idr, 0},
     {usd, 0}}}});
                // clang-format on
                REQUIRE_THROWS_AS(source.pathpay2(noAuthorizedDestinationTrust,
     idr, 10, idr, 10, {}), ex_PATH_PAYMENT_NOT_AUTHORIZED);
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance1 - 2 * txfee}, {idr, 10}, {usd,
     0}}}, {noAuthorizedDestinationTrust, {{xlm, minBalance1 - txfee}, {idr, 0},
     {usd, 0}}}});
                // clang-format on
            });
        }
        

        SECTION("path payment destination line full")
        {
            auto market = TestMarket{*app};
            auto source = root.create("source", minBalance1);
            auto destination = root.create("destination", minBalance1);
            source.changeTrust(idr, 20);
            destination.changeTrust(idr, 10);
            gateway.pay(source, idr, 10);
            gateway.pay(destination, idr, 10);
            for_all_versions(*app, [&] {
                REQUIRE_THROWS_AS(
                    gateway.pathpay2(destination, idr, 1, idr, 0, {}),
                                  ex_PATH_PAYMENT_LINE_FULL);
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance1 - txfee}, {idr, 10}, {usd,
     0}}}, {destination, {{xlm, minBalance1 - txfee}, {idr, 10}, {usd, 0}}}});
                // clang-format on
            });
        }

        SECTION("path payment destination line overflow")
        {
            auto market = TestMarket{*app};
            auto source = root.create("source", minBalance1);
            auto destination = root.create("destination", minBalance1);
            source.changeTrust(idr, 20);
            destination.changeTrust(idr, std::numeric_limits<int64_t>::max());
            gateway.pay(source, idr, 10);
            gateway.pay(destination, idr, std::numeric_limits<int64_t>::max() -
     10); for_all_versions(*app, [&] { REQUIRE_THROWS_AS(
                    gateway.pathpay2(destination, idr, 11, idr, 0, {}),
                                  ex_PATH_PAYMENT_LINE_FULL);
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance1 - txfee}, {idr, 10}, {usd,
     0}}}, {destination, {{xlm, minBalance1 - txfee}, {idr,
     std::numeric_limits<int64_t>::max() - 10}, {usd, 0}}}});
                // clang-format on
            });
        }
        

        

        SECTION("path payment send issuer missing")
        {
            auto market = TestMarket{*app};
            auto source = root.create("source", minBalance1);
            auto destination = root.create("destination", minBalance1);
            source.changeTrust(idr, 20);
            destination.changeTrust(usd, 20);
            gateway.pay(source, idr, 10);
            for_all_versions(*app, [&] {
                gateway.merge(root);
                REQUIRE_THROWS_AS(
                    source.pathpay2(destination, idr, 5, usd, 0, {}, &idr),
                    ex_PATH_PAYMENT_NO_ISSUER);
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance1 - 2 * txfee}, {idr, 10}, {usd,
     0}}}, {destination, {{xlm, minBalance1 - txfee}, {idr, 0}, {usd, 0}}}});
                // clang-format on
            });
        }
        

        SECTION("path payment middle issuer missing")
        {
            auto market = TestMarket{*app};
            auto source = root.create("source", minBalance1);
            auto destination = root.create("destination", minBalance1);
            source.changeTrust(idr, 20);
            destination.changeTrust(usd, 20);
            gateway.pay(source, idr, 10);
            for_all_versions(*app, [&] {
                auto btc = makeAsset(getAccount("missing"), "BTC");
                REQUIRE_THROWS_AS(
                    source.pathpay2(destination, idr, 5, usd, 0, {btc}, &btc),
                    ex_PATH_PAYMENT_NO_ISSUER);
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance1 - 2 * txfee}, {idr, 10}, {usd,
     0}}}, {destination, {{xlm, minBalance1 - txfee}, {idr, 0}, {usd, 0}}}});
                // clang-format on
            });
        }

        SECTION("path payment last issuer missing")
        {
            auto market = TestMarket{*app};
            auto source = root.create("source", minBalance1);
            auto destination = root.create("destination", minBalance1);
            source.changeTrust(idr, 20);
            destination.changeTrust(usd, 20);
            gateway.pay(source, idr, 10);
            for_all_versions(*app, [&] {
                gateway2.merge(root);
                REQUIRE_THROWS_AS(
                    source.pathpay2(destination, idr, 5, usd, 0, {}, &usd),
                    ex_PATH_PAYMENT_NO_ISSUER);
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance1 - 2 * txfee}, {idr, 10}, {usd,
     0}}}, {destination, {{xlm, minBalance1 - txfee}, {idr, 0}, {usd, 0}}}});
                // clang-format on
            });
        }

        SECTION("path payment not enough offers for first exchange")
        {
            auto market = TestMarket{*app};
            auto source = root.create("source", minBalance1);
            auto destination = root.create("destination", minBalance1);
            auto mm12 = root.create("mm12", minBalance3);
            auto mm23 = root.create("mm23", minBalance3);
            auto mm34 = root.create("mm34", minBalance3);

            source.changeTrust(cur1, 20);
            mm12.changeTrust(cur1, 20);
            mm12.changeTrust(cur2, 20);
            mm23.changeTrust(cur2, 20);
            mm23.changeTrust(cur3, 20);
            mm34.changeTrust(cur3, 20);
            mm34.changeTrust(cur4, 20);
            destination.changeTrust(cur4, 20);

            gateway.pay(source, cur1, 10);
            gateway.pay(mm12, cur2, 10);
            gateway2.pay(mm23, cur3, 10);
            gateway2.pay(mm34, cur4, 10);

            market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm12, {cur2, cur1, Price{1, 1}, 10});
            });
            market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm23, {cur3, cur2, Price{1, 1}, 10});
            });
            market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm34, {cur4, cur3, Price{1, 1}, 9});
            });

            for_all_versions(*app, [&] {
                REQUIRE_THROWS_AS(market.requireChanges(
                                      {},
                                      [&] {
                                          source.pathpay2(destination, cur1, 10,
                                                          cur4,
                                                     0, {cur1, cur2, cur3,
     cur4});
                                      }),
                                  ex_PATH_PAYMENT_TOO_FEW_OFFERS);
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance1 - 2 * txfee}, {cur1, 10},
     {cur2, 0}, {cur3, 0}, {cur4, 0}}}, {mm12, {{xlm, minBalance3 - 3 * txfee},
     {cur1, 0}, {cur2, 10}, {cur3, 0}, {cur4, 0}}}, {mm23, {{xlm, minBalance3 -
     3 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 10}, {cur4, 0}}}, {mm34, {{xlm,
     minBalance3 - 3 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 0}, {cur4, 10}}},
                     {destination, {{xlm, minBalance1 - txfee}, {cur1, 0},
     {cur2, 0}, {cur3, 0}, {cur4, 0}}}});
                // clang-format on
            });
        }

        SECTION("path payment not enough offers for middle exchange")
        {
            auto market = TestMarket{*app};
            auto source = root.create("source", minBalance1);
            auto destination = root.create("destination", minBalance1);
            auto mm12 = root.create("mm12", minBalance3);
            auto mm23 = root.create("mm23", minBalance3);
            auto mm34 = root.create("mm34", minBalance3);

            source.changeTrust(cur1, 20);
            mm12.changeTrust(cur1, 20);
            mm12.changeTrust(cur2, 20);
            mm23.changeTrust(cur2, 20);
            mm23.changeTrust(cur3, 20);
            mm34.changeTrust(cur3, 20);
            mm34.changeTrust(cur4, 20);
            destination.changeTrust(cur4, 20);

            gateway.pay(source, cur1, 10);
            gateway.pay(mm12, cur2, 10);
            gateway2.pay(mm23, cur3, 10);
            gateway2.pay(mm34, cur4, 10);

            market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm12, {cur2, cur1, Price{1, 1}, 10});
            });
            market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm23, {cur3, cur2, Price{1, 1}, 9});
            });
            market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm34, {cur4, cur3, Price{1, 1}, 10});
            });

            for_all_versions(*app, [&] {
                REQUIRE_THROWS_AS(market.requireChanges(
                                      {},
                                      [&] {
                                          source.pathpay2(destination, cur1, 10,
                                                          cur4,
                                                     0, {cur1, cur2, cur3,
     cur4});
                                      }),
                                  ex_PATH_PAYMENT_TOO_FEW_OFFERS);
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance1 - 2 * txfee}, {cur1, 10},
     {cur2, 0}, {cur3, 0}, {cur4, 0}}}, {mm12, {{xlm, minBalance3 - 3 * txfee},
     {cur1, 0}, {cur2, 10}, {cur3, 0}, {cur4, 0}}}, {mm23, {{xlm, minBalance3 -
     3 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 10}, {cur4, 0}}}, {mm34, {{xlm,
     minBalance3 - 3 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 0}, {cur4, 10}}},
                     {destination, {{xlm, minBalance1 - txfee}, {cur1, 0},
     {cur2, 0}, {cur3, 0}, {cur4, 0}}}});
                // clang-format on
            });
        }

        SECTION("path payment not enough offers for last exchange")
        {
            auto market = TestMarket{*app};
            auto source = root.create("source", minBalance1);
            auto destination = root.create("destination", minBalance1);
            auto mm12 = root.create("mm12", minBalance3);
            auto mm23 = root.create("mm23", minBalance3);
            auto mm34 = root.create("mm34", minBalance3);

            source.changeTrust(cur1, 20);
            mm12.changeTrust(cur1, 20);
            mm12.changeTrust(cur2, 20);
            mm23.changeTrust(cur2, 20);
            mm23.changeTrust(cur3, 20);
            mm34.changeTrust(cur3, 20);
            mm34.changeTrust(cur4, 20);
            destination.changeTrust(cur4, 20);

            gateway.pay(source, cur1, 10);
            gateway.pay(mm12, cur2, 10);
            gateway2.pay(mm23, cur3, 10);
            gateway2.pay(mm34, cur4, 10);

            market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm12, {cur2, cur1, Price{1, 1}, 9});
            });
            market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm23, {cur3, cur2, Price{1, 1}, 10});
            });
            market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm34, {cur4, cur3, Price{1, 1}, 10});
            });

            for_all_versions(*app, [&] {
                REQUIRE_THROWS_AS(market.requireChanges(
                                      {},
                                      [&] {
                                          source.pathpay2(destination, cur1, 10,
                                                          cur4,
                                                     0, {cur1, cur2, cur3,
     cur4});
                                      }),
                                  ex_PATH_PAYMENT_TOO_FEW_OFFERS);
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance1 - 2 * txfee}, {cur1, 10},
     {cur2, 0}, {cur3, 0}, {cur4, 0}}}, {mm12, {{xlm, minBalance3 - 3 * txfee},
     {cur1, 0}, {cur2, 10}, {cur3, 0}, {cur4, 0}}}, {mm23, {{xlm, minBalance3 -
     3 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 10}, {cur4, 0}}}, {mm34, {{xlm,
     minBalance3 - 3 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 0}, {cur4, 10}}},
                     {destination, {{xlm, minBalance1 - txfee}, {cur1, 0},
     {cur2, 0}, {cur3, 0}, {cur4, 0}}}});
                // clang-format on
            });
        }

        SECTION("path payment crosses own offer for first exchange")
        {
            auto market = TestMarket{*app};
            auto source = root.create("source", minBalance3);
            auto destination = root.create("destination", minBalance1);
            auto mm23 = root.create("mm23", minBalance3);
            auto mm34 = root.create("mm34", minBalance3);

            source.changeTrust(cur1, 20);
            source.changeTrust(cur2, 20);
            mm23.changeTrust(cur2, 20);
            mm23.changeTrust(cur3, 20);
            mm34.changeTrust(cur3, 20);
            mm34.changeTrust(cur4, 20);
            destination.changeTrust(cur4, 20);

            gateway.pay(source, cur1, 10);
            gateway.pay(source, cur2, 10);
            gateway2.pay(mm23, cur3, 10);
            gateway2.pay(mm34, cur4, 10);

            market.requireChangesWithOffer({}, [&] {
                return market.addOffer(source, {cur2, cur1, Price{1, 1}, 10});
            });
            market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm23, {cur3, cur2, Price{1, 1}, 10});
            });
            market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm34, {cur4, cur3, Price{1, 1}, 10});
            });

            for_all_versions(*app, [&] {
                REQUIRE_THROWS_AS(market.requireChanges(
                                      {},
                                      [&] {
                                          source.pathpay2(destination, cur1, 10,
                                                          cur4,
                                                     0, {cur1, cur2, cur3,
     cur4});
                                      }),
                                  ex_PATH_PAYMENT_OFFER_CROSS_SELF);
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance3 - 4 * txfee}, {cur1, 10},
     {cur2, 10}, {cur3, 0}, {cur4, 0}}}, {mm23, {{xlm, minBalance3 - 3 * txfee},
     {cur1, 0}, {cur2, 0}, {cur3, 10}, {cur4, 0}}}, {mm34, {{xlm, minBalance3 -
     3 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 0}, {cur4, 10}}}, {destination,
     {{xlm, minBalance1 - txfee}, {cur1, 0}, {cur2, 0}, {cur3, 0}, {cur4,
     0}}}});
                // clang-format on
            });
        }

        SECTION("path payment crosses own offer for middle exchange")
        {
            auto market = TestMarket{*app};
            auto source = root.create("source", minBalance4);
            auto destination = root.create("destination", minBalance1);
            auto mm12 = root.create("mm12", minBalance3);
            auto mm34 = root.create("mm34", minBalance3);

            source.changeTrust(cur1, 20);
            source.changeTrust(cur2, 20);
            source.changeTrust(cur3, 20);
            mm12.changeTrust(cur1, 20);
            mm12.changeTrust(cur2, 20);
            mm34.changeTrust(cur3, 20);
            mm34.changeTrust(cur4, 20);
            destination.changeTrust(cur4, 20);

            gateway.pay(source, cur1, 10);
            gateway.pay(mm12, cur2, 10);
            gateway2.pay(source, cur3, 10);
            gateway2.pay(mm34, cur4, 10);

            market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm12, {cur2, cur1, Price{1, 1}, 10});
            });
            market.requireChangesWithOffer({}, [&] {
                return market.addOffer(source, {cur3, cur2, Price{1, 1}, 10});
            });
            market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm34, {cur4, cur3, Price{1, 1}, 10});
            });

            for_all_versions(*app, [&] {
                REQUIRE_THROWS_AS(market.requireChanges(
                                      {},
                                      [&] {
                                          source.pathpay2(destination, cur1, 10,
                                                          cur4,
                                                     0, {cur1, cur2, cur3,
     cur4});
                                      }),
                                  ex_PATH_PAYMENT_OFFER_CROSS_SELF);
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance4 - 5 * txfee}, {cur1, 10},
     {cur2, 0}, {cur3, 10}, {cur4, 0}}}, {mm12, {{xlm, minBalance3 - 3 * txfee},
     {cur1, 0}, {cur2, 10}, {cur3, 0}, {cur4, 0}}}, {mm34, {{xlm, minBalance3 -
     3 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 0}, {cur4, 10}}}, {destination,
     {{xlm, minBalance1 - txfee}, {cur1, 0}, {cur2, 0}, {cur3, 0}, {cur4,
     0}}}});
                // clang-format on
            });
        }
        
        SECTION("path payment crosses own offer for last exchange")
        {
            auto market = TestMarket{*app};
            auto source = root.create("source", minBalance4);
            auto destination = root.create("destination", minBalance1);
            auto mm12 = root.create("mm12", minBalance3);
            auto mm23 = root.create("mm23", minBalance3);

            source.changeTrust(cur1, 20);
            source.changeTrust(cur3, 20);
            source.changeTrust(cur4, 20);
            mm12.changeTrust(cur1, 20);
            mm12.changeTrust(cur2, 20);
            mm23.changeTrust(cur2, 20);
            mm23.changeTrust(cur3, 20);
            destination.changeTrust(cur4, 20);

            gateway.pay(source, cur1, 10);
            gateway.pay(mm12, cur2, 10);
            gateway2.pay(mm23, cur3, 10);
            gateway2.pay(source, cur4, 10);

            market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm12, {cur2, cur1, Price{1, 1}, 10});
            });
            market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm23, {cur3, cur2, Price{1, 1}, 10});
            });
            market.requireChangesWithOffer({}, [&] {
                return market.addOffer(source, {cur4, cur3, Price{1, 1}, 10});
            });

            for_all_versions(*app, [&] {
                REQUIRE_THROWS_AS(market.requireChanges(
                                      {},
                                      [&] {
                                          source.pathpay2(destination, cur1, 10,
                                                          cur4,
                                                     0, {cur1, cur2, cur3,
     cur4});
                                      }),
                                  ex_PATH_PAYMENT_OFFER_CROSS_SELF);
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance4 - 5 * txfee}, {cur1, 10},
     {cur2, 0}, {cur3, 0}, {cur4, 10}}}, {mm12, {{xlm, minBalance3 - 3 * txfee},
     {cur1, 0}, {cur2, 10}, {cur3, 0}, {cur4, 0}}}, {mm23, {{xlm, minBalance3 -
     3 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 10}, {cur4, 0}}}, {destination,
     {{xlm, minBalance1 - txfee}, {cur1, 0}, {cur2, 0}, {cur3, 0}, {cur4,
     0}}}});
                // clang-format on
            });
        }
        

 
        SECTION("path payment does not cross own offer if better is available for "
            "first exchange")
        {
            auto market = TestMarket{*app};
            auto source = root.create("source", minBalance3);
            auto destination = root.create("destination", minBalance1);
            auto mm12 = root.create("mm12", minBalance3);
            auto mm23 = root.create("mm23", minBalance3);
            auto mm34 = root.create("mm34", minBalance3);

            source.changeTrust(cur1, 30);
            source.changeTrust(cur2, 30);
            mm12.changeTrust(cur1, 20);
            mm12.changeTrust(cur2, 20);
            mm23.changeTrust(cur2, 20);
            mm23.changeTrust(cur3, 20);
            mm34.changeTrust(cur3, 20);
            mm34.changeTrust(cur4, 20);
            destination.changeTrust(cur4, 20);

            gateway.pay(source, cur1, 10);
            gateway.pay(source, cur2, 10);
            gateway.pay(mm12, cur2, 10);
            gateway2.pay(mm23, cur3, 10);
            gateway2.pay(mm34, cur4, 10);

            market.requireChangesWithOffer({}, [&] {
                return market.addOffer(source, {cur2, cur1, Price{100, 99},
     10});
            });
            auto o1 = market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm12, {cur2, cur1, Price{1, 1}, 10});
            });
            auto o2 = market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm23, {cur3, cur2, Price{1, 1}, 10});
            });
            auto o3 = market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm34, {cur4, cur3, Price{1, 1}, 10});
            });

            for_all_versions(*app, [&] {
                auto actual = std::vector<ClaimOfferAtom>{};
                market.requireChanges({{o1.key, OfferState::DELETED},
                                       {o2.key, OfferState::DELETED},
                                       {o3.key, OfferState::DELETED}},
                                      [&] {
                                          actual =
                                              source
                                     .pathpay2(destination, cur1, 10, cur4,
                                                       0, {cur2, cur3})
                                                  .success()
                                                  .offers;
                                      });
                auto expected = std::vector<ClaimOfferAtom>{o3.exchanged(10,
     10), o2.exchanged(10, 10), o1.exchanged(10, 10)}; REQUIRE(actual ==
     expected);
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance3 - 4 * txfee}, {cur1, 0}, {cur2,
     10}, {cur3, 0}, {cur4, 0}}}, {mm12, {{xlm, minBalance3 - 3 * txfee}, {cur1,
     10}, {cur2, 0}, {cur3, 0}, {cur4, 0}}}, {mm23, {{xlm, minBalance3 - 3 *
     txfee}, {cur1, 0}, {cur2, 10}, {cur3, 0}, {cur4, 0}}}, {mm34, {{xlm,
     minBalance3 - 3 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 10}, {cur4, 0}}},
                     {destination, {{xlm, minBalance1 - txfee}, {cur1, 0},
     {cur2, 0}, {cur3, 0}, {cur4, 10}}}});
                // clang-format on
            });
        }
        

        SECTION("path payment does not cross own offer if better is available for "
            "middle exchange")
        {
            auto market = TestMarket{*app};
            auto source = root.create("source", minBalance4);
            auto destination = root.create("destination", minBalance1);
            auto mm12 = root.create("mm12", minBalance3);
            auto mm23 = root.create("mm23", minBalance3);
            auto mm34 = root.create("mm34", minBalance3);

            source.changeTrust(cur1, 30);
            source.changeTrust(cur2, 30);
            source.changeTrust(cur3, 30);
            mm12.changeTrust(cur1, 20);
            mm12.changeTrust(cur2, 20);
            mm23.changeTrust(cur2, 20);
            mm23.changeTrust(cur3, 20);
            mm34.changeTrust(cur3, 20);
            mm34.changeTrust(cur4, 20);
            destination.changeTrust(cur4, 20);

            gateway.pay(source, cur1, 10);
            gateway.pay(mm12, cur2, 10);
            gateway2.pay(source, cur3, 10);
            gateway2.pay(mm23, cur3, 10);
            gateway2.pay(mm34, cur4, 10);

            auto o1 = market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm12, {cur2, cur1, Price{1, 1}, 10});
            });
            auto o2 = market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm23, {cur3, cur2, Price{1, 1}, 10});
            });
            market.requireChangesWithOffer({}, [&] {
                return market.addOffer(source, {cur3, cur2, Price{100, 99},
     10});
            });
            auto o3 = market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm34, {cur4, cur3, Price{1, 1}, 10});
            });

            for_all_versions(*app, [&] {
                auto actual = std::vector<ClaimOfferAtom>{};
                market.requireChanges({{o1.key, OfferState::DELETED},
                                       {o2.key, OfferState::DELETED},
                                       {o3.key, OfferState::DELETED}},
                                      [&] {
                                          actual =
                                              source
                                     .pathpay2(destination, cur1, 10, cur4,
                                                       0, {cur1, cur2, cur3,
     cur4}) .success() .offers;
                                      });
                auto expected = std::vector<ClaimOfferAtom>{o3.exchanged(10,
     10), o2.exchanged(10, 10), o1.exchanged(10, 10)}; REQUIRE(actual ==
     expected);
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance4 - 5 * txfee}, {cur1, 0}, {cur2,
     0}, {cur3, 10}, {cur4, 0}}}, {mm12, {{xlm, minBalance3 - 3 * txfee}, {cur1,
     10}, {cur2, 0}, {cur3, 0}, {cur4, 0}}}, {mm23, {{xlm, minBalance3 - 3 *
     txfee}, {cur1, 0}, {cur2, 10}, {cur3, 0}, {cur4, 0}}}, {mm34, {{xlm,
     minBalance3 - 3 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 10}, {cur4, 0}}},
                     {destination, {{xlm, minBalance1 - txfee}, {cur1, 0},
     {cur2, 0}, {cur3, 0}, {cur4, 10}}}});
                // clang-format on
            });
        }

        SECTION("path payment does not cross own offer if better is available for "
            "last exchange")
        {
            auto market = TestMarket{*app};
            auto source = root.create("source", minBalance4);
            auto destination = root.create("destination", minBalance1);
            auto mm12 = root.create("mm12", minBalance3);
            auto mm23 = root.create("mm23", minBalance3);
            auto mm34 = root.create("mm34", minBalance3);

            source.changeTrust(cur1, 30);
            source.changeTrust(cur3, 30);
            source.changeTrust(cur4, 30);
            mm12.changeTrust(cur1, 20);
            mm12.changeTrust(cur2, 20);
            mm23.changeTrust(cur2, 20);
            mm23.changeTrust(cur3, 20);
            mm34.changeTrust(cur3, 20);
            mm34.changeTrust(cur4, 20);
            destination.changeTrust(cur4, 20);

            gateway.pay(source, cur1, 10);
            gateway.pay(mm12, cur2, 10);
            gateway2.pay(mm23, cur3, 10);
            gateway2.pay(source, cur4, 10);
            gateway2.pay(mm34, cur4, 10);

            auto o1 = market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm12, {cur2, cur1, Price{1, 1}, 10});
            });
            auto o2 = market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm23, {cur3, cur2, Price{1, 1}, 10});
            });
            auto o3 = market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm34, {cur4, cur3, Price{1, 1}, 10});
            });
            market.requireChangesWithOffer({}, [&] {
                return market.addOffer(source, {cur4, cur3, Price{100, 99},
     10});
            });

            for_all_versions(*app, [&] {
                auto actual = std::vector<ClaimOfferAtom>{};
                market.requireChanges({{o1.key, OfferState::DELETED},
                                       {o2.key, OfferState::DELETED},
                                       {o3.key, OfferState::DELETED}},
                                      [&] {
                                          actual =
                                              source
                                     .pathpay2(destination, cur1, 10, cur4,
                                                       0, {cur1, cur2, cur3,
     cur4}) .success() .offers;
                                      });
                auto expected = std::vector<ClaimOfferAtom>{o3.exchanged(10,
     10), o2.exchanged(10, 10), o1.exchanged(10, 10)}; REQUIRE(actual ==
     expected);
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance4 - 5 * txfee}, {cur1, 0}, {cur2,
     0}, {cur3, 0}, {cur4, 10}}}, {mm12, {{xlm, minBalance3 - 3 * txfee}, {cur1,
     10}, {cur2, 0}, {cur3, 0}, {cur4, 0}}}, {mm23, {{xlm, minBalance3 - 3 *
     txfee}, {cur1, 0}, {cur2, 10}, {cur3, 0}, {cur4, 0}}}, {mm34, {{xlm,
     minBalance3 - 3 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 10}, {cur4, 0}}},
                     {destination, {{xlm, minBalance1 - txfee}, {cur1, 0},
     {cur2, 0}, {cur3, 0}, {cur4, 10}}}});
                // clang-format on
            });
        }
        

        

        SECTION("path payment below destAmountMin XLM")
        {
            auto market = TestMarket{*app};
            auto source = root.create("source", minBalance);
            auto destination = root.create("destination", minBalance);
            for_all_versions(*app, [&] {
                REQUIRE_THROWS_AS(
                    source.pathpay2(destination, xlm, 10, xlm, 11, {}),
                                  ex_PATH_PAYMENT_OVER_SENDMAX);
                market.requireBalances(
                    {{source, {{xlm, minBalance - txfee}, {idr, 0}, {usd, 0}}},
                     {destination, {{xlm, minBalance}, {idr, 0}, {usd, 0}}}});
            });
        }

        SECTION("path payment below destAmountMin asset")
        {
            auto market = TestMarket{*app};
            auto source = root.create("source", minBalance1);
            auto destination = root.create("destination", minBalance1);
            source.changeTrust(idr, 10);
            destination.changeTrust(idr, 10);
            gateway.pay(source, idr, 10);

            for_all_versions(*app, [&] {
                REQUIRE_THROWS_AS(source.pathpay2(destination, idr, 9, idr, 10,
     {}), ex_PATH_PAYMENT_OVER_SENDMAX);
                // clang-format off
                market.requireBalances(
                    {{source, {{xlm, minBalance1 - 2 * txfee}, {idr, 10}, {usd,
     0}}}, {destination, {{xlm, minBalance1 - txfee}, {idr, 0}, {usd, 0}}}});
                // clang-format on
            });
        }
        
    
    SECTION("path payment below destAmountMin with real path")
    {
        auto market = TestMarket{*app};
        auto source = root.create("source", minBalance4);
        auto destination = root.create("destination", minBalance1);
        auto mm12 = root.create("mm12", minBalance3);
        auto mm23 = root.create("mm23", minBalance3);
        auto mm34 = root.create("mm34", minBalance3);

        source.changeTrust(cur1, 200);
        mm12.changeTrust(cur1, 200);
        mm12.changeTrust(cur2, 200);
        mm23.changeTrust(cur2, 200);
        mm23.changeTrust(cur3, 200);
        mm34.changeTrust(cur3, 200);
        mm34.changeTrust(cur4, 200);
        destination.changeTrust(cur4, 200);

        gateway.pay(source, cur1, 10);
        gateway.pay(mm12, cur2, 20);
        gateway2.pay(mm23, cur3, 40);
        gateway2.pay(mm34, cur4, 80);

        market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm12, {cur2, cur1, Price{1, 2}, 20});
        });
        market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm23, {cur3, cur2, Price{1, 2}, 40});
        });
        market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm34, {cur4, cur3, Price{1, 2}, 80});
        });

        for_all_versions(*app, [&] {
            REQUIRE_THROWS_AS(market.requireChanges(
                                  {},
                                  [&] {
                                      source.pathpay2(destination, cur1, 10,
    cur4, 81, {cur2, cur3});
                                  }),
                              ex_PATH_PAYMENT_OVER_SENDMAX);
            // clang-format off
            market.requireBalances(
                {{source, {{xlm, minBalance4 - 2 * txfee}, {cur1, 10}, {cur2,
    0}, {cur3, 0}, {cur4, 0}}}, {mm12, {{xlm, minBalance3 - 3 * txfee}, {cur1,
    0}, {cur2, 20}, {cur3, 0}, {cur4, 0}}}, {mm23, {{xlm, minBalance3 - 3 *
    txfee}, {cur1, 0}, {cur2, 0}, {cur3, 40}, {cur4, 0}}}, {mm34, {{xlm,
    minBalance3 - 3 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 0}, {cur4, 80}}},
                 {destination, {{xlm, minBalance1 - txfee}, {cur1, 0}, {cur2,
    0}, {cur3, 0}, {cur4, 0}}}});
            // clang-format on
        });
    }


    


    SECTION("path payment to self XLM")
    {
        auto market = TestMarket{*app};
        auto account = root.create("account", minBalance + txfee + 20);

        for_all_versions(*app, [&] {
            account.pathpay2(account, xlm, 20, xlm, 0, {});
            market.requireBalances({{account, {{xlm, minBalance + 20}}}});
        });
    }

    SECTION("path payment to self asset")
    {
        auto market = TestMarket{*app};
        auto account = root.create("account", minBalance1 + 2 * txfee);
        account.changeTrust(idr, 20);
        gateway.pay(account, idr, 10);

        for_all_versions(*app, [&] {
            auto offers = account.pathpay2(account, idr, 10, idr, 0, {});
            auto expected = std::vector<ClaimOfferAtom>{};
            REQUIRE(offers.success().offers == expected);
            market.requireBalances({{account, {{idr, 10}}}});
        });
    }
    
    /*
    // TODOJED: this is how it is handled in PathPaymentFixEnd
    SECTION("path payment to self asset over the limit")
    {
        auto market = TestMarket{*app};
        auto account = root.create("account", minBalance1 + 2 * txfee);
        account.changeTrust(idr, 20);
        gateway.pay(account, idr, 19);

        for_all_versions(*app, [&] {
            REQUIRE_THROWS_AS(account.pathpay2(account, idr, 2, idr, 0, {}),
                              ex_PATH_PAYMENT_LINE_FULL);
            market.requireBalances({{account, {{idr, 19}}}});
        });
    }
    */
    


    // TODOJED: different behavior from PathPaymentFixEnd
    SECTION("path payment to self asset over the limit")
    {
        auto market = TestMarket{*app};
        auto account = root.create("account", minBalance1 + 2 * txfee);
        account.changeTrust(idr, 20);
        gateway.pay(account, idr, 19);

        for_all_versions(*app, [&] {
            account.pathpay2(account, idr, 2, idr, 0, {});
            market.requireBalances({{account, {{idr, 19}}}});
        });
    }
    
    SECTION("path payment crosses destination offer for first exchange")
    {
        auto market = TestMarket{*app};
        auto source = root.create("source", minBalance1);
        auto destination = root.create("destination", minBalance4);
        auto mm23 = root.create("mm23", minBalance3);
        auto mm34 = root.create("mm34", minBalance3);

        source.changeTrust(cur1, 20);
        mm23.changeTrust(cur2, 20);
        mm23.changeTrust(cur3, 20);
        mm34.changeTrust(cur3, 20);
        mm34.changeTrust(cur4, 20);
        destination.changeTrust(cur1, 20);
        destination.changeTrust(cur2, 20);
        destination.changeTrust(cur4, 20);

        gateway.pay(source, cur1, 10);
        gateway.pay(destination, cur2, 10);
        gateway2.pay(mm23, cur3, 10);
        gateway2.pay(mm34, cur4, 10);

        auto o1 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(destination, {cur2, cur1, Price{1, 1}, 10});
        });
        auto o2 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm23, {cur3, cur2, Price{1, 1}, 10});
        });
        auto o3 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm34, {cur4, cur3, Price{1, 1}, 10});
        });

        for_all_versions(*app, [&] {
            auto actual = std::vector<ClaimOfferAtom>{};
            market.requireChanges(
                {{o1.key, OfferState::DELETED},
                 {o2.key, OfferState::DELETED},
                 {o3.key, OfferState::DELETED}},
                [&] {
                    actual = source
                                 .pathpay2(destination, cur1, 10, cur4, 0,
                                           {cur2, cur3})
                                 .success()
                                 .offers;
                });
            auto expected = std::vector<ClaimOfferAtom>{o3.exchanged(10, 10),
                                                        o2.exchanged(10, 10),
                                                        o1.exchanged(10, 10)};
            REQUIRE(actual == expected);
            // clang-format off
            market.requireBalances(
                {{source, {{xlm, minBalance1 - 2 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 0}, {cur4, 0}}},
                 {mm23, {{xlm, minBalance3 - 3 * txfee}, {cur1, 0}, {cur2, 10}, {cur3, 0}, {cur4, 0}}},
                 {mm34, {{xlm, minBalance3 - 3 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 10}, {cur4, 0}}},
                 {destination, {{xlm, minBalance4 - 4 * txfee}, {cur1, 10}, {cur2, 0}, {cur3, 0}, {cur4, 10}}}});
            // clang-format on
        });
    }

    SECTION("path payment crosses destination offer for middle exchange")
    {
        auto market = TestMarket{*app};
        auto source = root.create("source", minBalance1);
        auto destination = root.create("destination", minBalance4);
        auto mm12 = root.create("mm12", minBalance3);
        auto mm34 = root.create("mm34", minBalance3);

        source.changeTrust(cur1, 20);
        mm12.changeTrust(cur1, 20);
        mm12.changeTrust(cur2, 20);
        mm34.changeTrust(cur3, 20);
        mm34.changeTrust(cur4, 20);
        destination.changeTrust(cur2, 20);
        destination.changeTrust(cur3, 20);
        destination.changeTrust(cur4, 20);

        gateway.pay(source, cur1, 10);
        gateway.pay(mm12, cur2, 10);
        gateway2.pay(destination, cur3, 10);
        gateway2.pay(mm34, cur4, 10);

        auto o1 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm12, {cur2, cur1, Price{1, 1}, 10});
        });
        auto o2 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(destination, {cur3, cur2, Price{1, 1}, 10});
        });
        auto o3 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm34, {cur4, cur3, Price{1, 1}, 10});
        });

        for_all_versions(*app, [&] {
            auto actual = std::vector<ClaimOfferAtom>{};
            market.requireChanges(
                {{o1.key, OfferState::DELETED},
                 {o2.key, OfferState::DELETED},
                 {o3.key, OfferState::DELETED}},
                [&] {
                    actual = source
                                 .pathpay2(destination, cur1, 10, cur4, 0,
                                           {cur2, cur3})
                                 .success()
                                 .offers;
                });
            auto expected = std::vector<ClaimOfferAtom>{o3.exchanged(10, 10),
                                                        o2.exchanged(10, 10),
                                                        o1.exchanged(10, 10)};
            REQUIRE(actual == expected);
            // clang-format off
            market.requireBalances(
                {{source, {{xlm, minBalance1 - 2 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 0}, {cur4, 0}}},
                 {mm12, {{xlm, minBalance3 - 3 * txfee}, {cur1, 10}, {cur2, 0}, {cur3, 0}, {cur4, 0}}},
                 {mm34, {{xlm, minBalance3 - 3 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 10}, {cur4, 0}}},
                 {destination, {{xlm, minBalance4 - 4 * txfee}, {cur1, 0}, {cur2, 10}, {cur3, 0}, {cur4, 10}}}});
            // clang-format on
        });
    }

    SECTION("path payment crosses destination offer for last exchange")
    {
        auto market = TestMarket{*app};
        auto source = root.create("source", minBalance1);
        auto destination = root.create("destination", minBalance4);
        auto mm12 = root.create("mm12", minBalance3);
        auto mm23 = root.create("mm23", minBalance3);

        source.changeTrust(cur1, 20);
        mm12.changeTrust(cur1, 20);
        mm12.changeTrust(cur2, 20);
        mm23.changeTrust(cur2, 20);
        mm23.changeTrust(cur3, 20);
        destination.changeTrust(cur3, 20);
        destination.changeTrust(cur4, 20);

        gateway.pay(source, cur1, 10);
        gateway.pay(mm12, cur2, 10);
        gateway2.pay(mm23, cur3, 10);
        gateway2.pay(destination, cur4, 10);

        auto o1 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm12, {cur2, cur1, Price{1, 1}, 10});
        });
        auto o2 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm23, {cur3, cur2, Price{1, 1}, 10});
        });
        auto o3 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(destination, {cur4, cur3, Price{1, 1}, 10});
        });

        for_all_versions(*app, [&] {
            auto actual = std::vector<ClaimOfferAtom>{};
            market.requireChanges(
                {{o1.key, OfferState::DELETED},
                 {o2.key, OfferState::DELETED},
                 {o3.key, OfferState::DELETED}},
                [&] {
                    actual = source
                                 .pathpay2(destination, cur1, 10, cur4, 10,
                                           {cur1, cur2, cur3, cur4})
                                 .success()
                                 .offers;
                });
            auto expected = std::vector<ClaimOfferAtom>{o3.exchanged(10, 10),
                                                        o2.exchanged(10, 10),
                                                        o1.exchanged(10, 10)};
            REQUIRE(actual == expected);
            // clang-format off
            market.requireBalances(
                {{source, {{xlm, minBalance1 - 2 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 0}, {cur4, 0}}},
                 {mm12, {{xlm, minBalance3 - 3 * txfee}, {cur1, 10}, {cur2, 0}, {cur3, 0}, {cur4, 0}}},
                 {mm23, {{xlm, minBalance3 - 3 * txfee}, {cur1, 0}, {cur2, 10}, {cur3, 0}, {cur4, 0}}},
                 {destination, {{xlm, minBalance4 - 3 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 10}, {cur4, 10}}}});
            // clang-format on
        });
    }
    
    SECTION("path payment uses whole best offer for first exchange")
    {
        auto market = TestMarket{*app};
        auto source = root.create("source", minBalance4);
        auto destination = root.create("destination", minBalance1);
        auto mm12a = root.create("mm12a", minBalance3);
        auto mm12b = root.create("mm12b", minBalance3);
        auto mm23 = root.create("mm23", minBalance3);
        auto mm34 = root.create("mm34", minBalance3);

        source.changeTrust(cur1, 200);
        mm12a.changeTrust(cur1, 200);
        mm12a.changeTrust(cur2, 200);
        mm12b.changeTrust(cur1, 200);
        mm12b.changeTrust(cur2, 200);
        mm23.changeTrust(cur2, 200);
        mm23.changeTrust(cur3, 200);
        mm34.changeTrust(cur3, 200);
        mm34.changeTrust(cur4, 200);
        destination.changeTrust(cur4, 200);

        gateway.pay(source, cur1, 80);
        gateway.pay(mm12a, cur2, 40);
        gateway.pay(mm12b, cur2, 40);
        gateway2.pay(mm23, cur3, 20);
        gateway2.pay(mm34, cur4, 10);

        auto o1a = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm12a, {cur2, cur1, Price{2, 1}, 10});
        });
        auto o1b = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm12b, {cur2, cur1, Price{2, 1}, 40});
        });
        auto o2 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm23, {cur3, cur2, Price{2, 1}, 20});
        });
        auto o3 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm34, {cur4, cur3, Price{2, 1}, 10});
        });

        for_all_versions(*app, [&] {
            auto actual = std::vector<ClaimOfferAtom>{};
            market.requireChanges(
                {{o1a.key, OfferState::DELETED},
                 {o1b.key, {cur2, cur1, Price{2, 1}, 10}},
                 {o2.key, OfferState::DELETED},
                 {o3.key, OfferState::DELETED}},
                [&] {
                    actual = source
                                 .pathpay2(destination, cur1, 80, cur4, 10,
                                           {cur2, cur3})
                                 .success()
                                 .offers;
                });
            auto expected = std::vector<ClaimOfferAtom>{
                o3.exchanged(10, 20),
                o2.exchanged(20, 40),
                o1a.exchanged(10, 20),
                o1b.exchanged(30, 60)  };

            REQUIRE(actual == expected);
            // clang-format off
            market.requireBalances(
                {{source, {{xlm, minBalance4 - 2 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 0}, {cur4, 0}}},
                 {mm12a, {{xlm, minBalance3 - 3 * txfee}, {cur1, 20}, {cur2, 30}, {cur3, 0}, {cur4, 0}}},
                 {mm12b, {{xlm, minBalance3 - 3 * txfee}, {cur1, 60}, {cur2, 10}, {cur3, 0}, {cur4, 0}}},
                 {mm23, {{xlm, minBalance3 - 3 * txfee}, {cur1, 0}, {cur2, 40}, {cur3, 0}, {cur4, 0}}},
                 {mm34, {{xlm, minBalance3 - 3 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 20}, {cur4, 0}}},
                 {destination, {{xlm, minBalance1 - txfee}, {cur1, 0}, {cur2, 0}, {cur3, 0}, {cur4, 10}}}});
            // clang-format on
        });
    }

    SECTION("path payment uses whole best offer for second exchange")
    {
        auto market = TestMarket{*app};
        auto source = root.create("source", minBalance4);
        auto destination = root.create("destination", minBalance1);
        auto mm12 = root.create("mm12a", minBalance3);
        auto mm23a = root.create("mm23a", minBalance3);
        auto mm23b = root.create("mm23b", minBalance3);
        auto mm34 = root.create("mm34", minBalance3);

        source.changeTrust(cur1, 200);
        mm12.changeTrust(cur1, 200);
        mm12.changeTrust(cur2, 200);
        mm23a.changeTrust(cur2, 200);
        mm23a.changeTrust(cur3, 200);
        mm23b.changeTrust(cur2, 200);
        mm23b.changeTrust(cur3, 200);
        mm34.changeTrust(cur3, 200);
        mm34.changeTrust(cur4, 200);
        destination.changeTrust(cur4, 200);

        gateway.pay(source, cur1, 80);
        gateway.pay(mm12, cur2, 40);
        gateway2.pay(mm23a, cur3, 20);
        gateway2.pay(mm23b, cur3, 20);
        gateway2.pay(mm34, cur4, 10);

        auto o1 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm12, {cur2, cur1, Price{2, 1}, 40});
        });
        auto o2a = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm23a, {cur3, cur2, Price{2, 1}, 15});
        });
        auto o2b = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm23b, {cur3, cur2, Price{2, 1}, 10});
        });
        auto o3 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm34, {cur4, cur3, Price{2, 1}, 10});
        });

        for_all_versions(*app, [&] {
            auto actual = std::vector<ClaimOfferAtom>{};
            market.requireChanges(
                {{o1.key, OfferState::DELETED},
                 {o2a.key, OfferState::DELETED},
                 {o2b.key, {cur3, cur2, Price{2, 1}, 5}},
                 {o3.key, OfferState::DELETED}},
                [&] {
                    actual = source
                                 .pathpay2(destination, cur1, 80, cur4, 10,
                                           {cur2, cur3})
                                 .success()
                                 .offers;
                });
            auto expected = std::vector<ClaimOfferAtom>{
                o3.exchanged(10, 20),
                o2a.exchanged(15, 30),
                o2b.exchanged(5, 10),
                o1.exchanged(40, 80)};

            REQUIRE(actual == expected);
            // clang-format off
            market.requireBalances(
                {{source, {{xlm, minBalance4 - 2 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 0}, {cur4, 0}}},
                 {mm12, {{xlm, minBalance3 - 3 * txfee}, {cur1, 80}, {cur2, 0}, {cur3, 0}, {cur4, 0}}},
                 {mm23a, {{xlm, minBalance3 - 3 * txfee}, {cur1, 0}, {cur2, 30}, {cur3, 5}, {cur4, 0}}},
                 {mm23a, {{xlm, minBalance3 - 3 * txfee}, {cur1, 0}, {cur2, 30}, {cur3, 5}, {cur4, 0}}},
                 {mm34, {{xlm, minBalance3 - 3 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 20}, {cur4, 0}}},
                 {destination, {{xlm, minBalance1 - txfee}, {cur1, 0}, {cur2, 0}, {cur3, 0}, {cur4, 10}}}});
            // clang-format on
        });
    }
    
    SECTION("path payment uses whole best offer for last exchange")
    {
        auto market = TestMarket{*app};
        auto source = root.create("source", minBalance4);
        auto destination = root.create("destination", minBalance1);
        auto mm12 = root.create("mm12a", minBalance3);
        auto mm23 = root.create("mm23", minBalance3);
        auto mm34a = root.create("mm34a", minBalance3);
        auto mm34b = root.create("mm34b", minBalance3);

        source.changeTrust(cur1, 200);
        mm12.changeTrust(cur1, 200);
        mm12.changeTrust(cur2, 200);
        mm23.changeTrust(cur2, 200);
        mm23.changeTrust(cur3, 200);
        mm34a.changeTrust(cur3, 200);
        mm34a.changeTrust(cur4, 200);
        mm34b.changeTrust(cur3, 200);
        mm34b.changeTrust(cur4, 200);
        destination.changeTrust(cur4, 200);

        gateway.pay(source, cur1, 80);
        gateway.pay(mm12, cur2, 40);
        gateway2.pay(mm23, cur3, 20);
        gateway2.pay(mm34a, cur4, 10);
        gateway2.pay(mm34b, cur4, 10);

        auto o1 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm12, {cur2, cur1, Price{2, 1}, 40});
        });
        auto o2 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm23, {cur3, cur2, Price{2, 1}, 20});
        });
        auto o3a = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm34a, {cur4, cur3, Price{2, 1}, 2});
        });
        auto o3b = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm34b, {cur4, cur3, Price{2, 1}, 10});
        });

        for_all_versions(*app, [&] {
            auto actual = std::vector<ClaimOfferAtom>{};
            market.requireChanges(
                {{o1.key, OfferState::DELETED},
                 {o2.key, OfferState::DELETED},
                 {o3a.key, OfferState::DELETED},
                 {o3b.key, {cur4, cur3, Price{2, 1}, 2}}},
                [&] {
                    actual = source
                                 .pathpay2(destination, cur1, 80, cur4, 10,
                                           {cur1, cur2, cur3, cur4})
                                 .success()
                                 .offers;
                });
            auto expected = std::vector<ClaimOfferAtom>{
                o3a.exchanged(2, 4), 
                o3b.exchanged(8, 16),
                o2.exchanged(20, 40), 
                o1.exchanged(40, 80) };

            REQUIRE(actual == expected);
            // clang-format off
            market.requireBalances(
                {{source, {{xlm, minBalance4 - 2 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 0}, {cur4, 0}}},
                 {mm12, {{xlm, minBalance3 - 3 * txfee}, {cur1, 80}, {cur2, 0}, {cur3, 0}, {cur4, 0}}},
                 {mm23, {{xlm, minBalance3 - 3 * txfee}, {cur1, 0}, {cur2, 40}, {cur3, 0}, {cur4, 0}}},
                 {mm34a, {{xlm, minBalance3 - 3 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 4}, {cur4, 8}}},
                 {mm34b, {{xlm, minBalance3 - 3 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 16}, {cur4, 2}}},
                 {destination, {{xlm, minBalance1 - txfee}, {cur1, 0}, {cur2, 0}, {cur3, 0}, {cur4, 10}}}});
            // clang-format on
        });
    }

    SECTION("path payment reaches limit for offer for first exchange")
    {
        auto market = TestMarket{*app};
        auto source = root.create("source", minBalance4);
        auto destination = root.create("destination", minBalance1);
        auto mm12a = root.create("mm12a", minBalance3);
        auto mm12b = root.create("mm12b", minBalance3);
        auto mm23 = root.create("mm23", minBalance3);
        auto mm34 = root.create("mm34", minBalance3);

        source.changeTrust(cur1, 200);
        mm12a.changeTrust(cur1, 200);
        mm12a.changeTrust(cur2, 200);
        mm12b.changeTrust(cur1, 200);
        mm12b.changeTrust(cur2, 200);
        mm23.changeTrust(cur2, 200);
        mm23.changeTrust(cur3, 200);
        mm34.changeTrust(cur3, 200);
        mm34.changeTrust(cur4, 200);
        destination.changeTrust(cur4, 200);

        gateway.pay(source, cur1, 80);
        gateway.pay(mm12a, cur2, 40);
        gateway.pay(mm12b, cur2, 40);
        gateway2.pay(mm23, cur3, 20);
        gateway2.pay(mm34, cur4, 10);

        auto o1a = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm12a, {cur2, cur1, Price{2, 1}, 40});
        });
        auto o1b = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm12b, {cur2, cur1, Price{2, 1}, 40});
        });
        auto o2 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm23, {cur3, cur2, Price{2, 1}, 20});
        });
        auto o3 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm34, {cur4, cur3, Price{2, 1}, 10});
        });

        for_all_versions( *app, [&] {
            REQUIRE_THROWS_AS(mm12a.changeTrust(cur1, 5),
                              ex_CHANGE_TRUST_INVALID_LIMIT);
        });
    }

    SECTION("path payment reaches limit for offer for second exchange")
    {
        auto market = TestMarket{*app};
        auto source = root.create("source", minBalance4);
        auto destination = root.create("destination", minBalance1);
        auto mm12 = root.create("mm12a", minBalance3);
        auto mm23a = root.create("mm23a", minBalance3);
        auto mm23b = root.create("mm23b", minBalance3);
        auto mm34 = root.create("mm34", minBalance3);

        source.changeTrust(cur1, 200);
        mm12.changeTrust(cur1, 200);
        mm12.changeTrust(cur2, 200);
        mm23a.changeTrust(cur2, 200);
        mm23a.changeTrust(cur3, 200);
        mm23b.changeTrust(cur2, 200);
        mm23b.changeTrust(cur3, 200);
        mm34.changeTrust(cur3, 200);
        mm34.changeTrust(cur4, 200);
        destination.changeTrust(cur4, 200);

        gateway.pay(source, cur1, 80);
        gateway.pay(mm12, cur2, 40);
        gateway2.pay(mm23a, cur3, 20);
        gateway2.pay(mm23b, cur3, 20);
        gateway2.pay(mm34, cur4, 10);

        auto o1 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm12, {cur2, cur1, Price{2, 1}, 40});
        });
        auto o2a = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm23a, {cur3, cur2, Price{2, 1}, 20});
        });
        auto o2b = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm23b, {cur3, cur2, Price{2, 1}, 20});
        });
        auto o3 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm34, {cur4, cur3, Price{2, 1}, 10});
        });

        for_all_versions(*app, [&] {
            REQUIRE_THROWS_AS(mm23a.changeTrust(cur2, 5),
                              ex_CHANGE_TRUST_INVALID_LIMIT);
        });
    }

    SECTION("path payment reaches limit for offer for last exchange")
    {
        auto market = TestMarket{*app};
        auto source = root.create("source", minBalance4);
        auto destination = root.create("destination", minBalance1);
        auto mm12 = root.create("mm12a", minBalance3);
        auto mm23 = root.create("mm23", minBalance3);
        auto mm34a = root.create("mm34a", minBalance3);
        auto mm34b = root.create("mm34b", minBalance3);

        source.changeTrust(cur1, 200);
        mm12.changeTrust(cur1, 200);
        mm12.changeTrust(cur2, 200);
        mm23.changeTrust(cur2, 200);
        mm23.changeTrust(cur3, 200);
        mm34a.changeTrust(cur3, 200);
        mm34a.changeTrust(cur4, 200);
        mm34b.changeTrust(cur3, 200);
        mm34b.changeTrust(cur4, 200);
        destination.changeTrust(cur4, 200);

        gateway.pay(source, cur1, 80);
        gateway.pay(mm12, cur2, 40);
        gateway2.pay(mm23, cur3, 20);
        gateway2.pay(mm34a, cur4, 10);
        gateway2.pay(mm34b, cur4, 10);

        auto o1 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm12, {cur2, cur1, Price{2, 1}, 40});
        });
        auto o2 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm23, {cur3, cur2, Price{2, 1}, 20});
        });
        auto o3a = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm34a, {cur4, cur3, Price{2, 1}, 10});
        });
        auto o3b = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm34b, {cur4, cur3, Price{2, 1}, 10});
        });

        for_all_versions( *app, [&] {
            REQUIRE_THROWS_AS(mm34a.changeTrust(cur3, 2),
                              ex_CHANGE_TRUST_INVALID_LIMIT);
        });
    }

    SECTION("path payment missing trust line for offer for first exchange")
    {
        auto market = TestMarket{*app};
        auto source = root.create("source", minBalance4);
        auto destination = root.create("destination", minBalance1);
        auto mm12a = root.create("mm12a", minBalance3);
        auto mm12b = root.create("mm12b", minBalance3);
        auto mm23 = root.create("mm23", minBalance3);
        auto mm34 = root.create("mm34", minBalance3);

        source.changeTrust(cur1, 200);
        mm12a.changeTrust(cur1, 200);
        mm12a.changeTrust(cur2, 200);
        mm12b.changeTrust(cur1, 200);
        mm12b.changeTrust(cur2, 200);
        mm23.changeTrust(cur2, 200);
        mm23.changeTrust(cur3, 200);
        mm34.changeTrust(cur3, 200);
        mm34.changeTrust(cur4, 200);
        destination.changeTrust(cur4, 200);

        gateway.pay(source, cur1, 80);
        gateway.pay(mm12a, cur2, 40);
        gateway.pay(mm12b, cur2, 40);
        gateway2.pay(mm23, cur3, 20);
        gateway2.pay(mm34, cur4, 10);

        auto o1a = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm12a, {cur2, cur1, Price{2, 1}, 40});
        });
        auto o1b = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm12b, {cur2, cur1, Price{2, 1}, 40});
        });
        auto o2 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm23, {cur3, cur2, Price{2, 1}, 20});
        });
        auto o3 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm34, {cur4, cur3, Price{2, 1}, 10});
        });

        SECTION("missing selling line")
        {
            
            for_all_versions(*app, [&] {
                REQUIRE_THROWS_AS(mm12a.pay(gateway, cur2, 40),
                                  ex_PAYMENT_UNDERFUNDED);
            });
        }

        SECTION("missing buying line")
        {
            for_all_versions(*app, [&] {
                REQUIRE_THROWS_AS(mm12a.changeTrust(cur1, 0),
                                  ex_CHANGE_TRUST_INVALID_LIMIT);
            });
        }
    }

    SECTION("path payment missing trust line for offer for second exchange")
    {
        auto market = TestMarket{*app};
        auto source = root.create("source", minBalance4);
        auto destination = root.create("destination", minBalance1);
        auto mm12 = root.create("mm12a", minBalance3);
        auto mm23a = root.create("mm23a", minBalance3);
        auto mm23b = root.create("mm23b", minBalance3);
        auto mm34 = root.create("mm34", minBalance3);

        source.changeTrust(cur1, 200);
        mm12.changeTrust(cur1, 200);
        mm12.changeTrust(cur2, 200);
        mm23a.changeTrust(cur2, 200);
        mm23a.changeTrust(cur3, 200);
        mm23b.changeTrust(cur2, 200);
        mm23b.changeTrust(cur3, 200);
        mm34.changeTrust(cur3, 200);
        mm34.changeTrust(cur4, 200);
        destination.changeTrust(cur4, 200);

        gateway.pay(source, cur1, 80);
        gateway.pay(mm12, cur2, 40);
        gateway2.pay(mm23a, cur3, 20);
        gateway2.pay(mm23b, cur3, 20);
        gateway2.pay(mm34, cur4, 10);

        auto o1 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm12, {cur2, cur1, Price{2, 1}, 40});
        });
        auto o2a = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm23a, {cur3, cur2, Price{2, 1}, 20});
        });
        auto o2b = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm23b, {cur3, cur2, Price{2, 1}, 20});
        });
        auto o3 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm34, {cur4, cur3, Price{2, 1}, 10});
        });

        SECTION("missing selling line")
        {
            for_all_versions( *app, [&] {
                REQUIRE_THROWS_AS(mm23a.pay(gateway2, cur3, 20),
                                  ex_PAYMENT_UNDERFUNDED);
            });
        }

        SECTION("missing buying line")
        {
            for_all_versions( *app, [&] {
                REQUIRE_THROWS_AS(mm23a.changeTrust(cur2, 0),
                                  ex_CHANGE_TRUST_INVALID_LIMIT);
            });
        }
    }

    SECTION("path payment missing trust line for offer for last exchange")
    {
        auto market = TestMarket{*app};
        auto source = root.create("source", minBalance4);
        auto destination = root.create("destination", minBalance1);
        auto mm12 = root.create("mm12a", minBalance3);
        auto mm23 = root.create("mm23", minBalance3);
        auto mm34a = root.create("mm34a", minBalance3);
        auto mm34b = root.create("mm34b", minBalance3);

        source.changeTrust(cur1, 200);
        mm12.changeTrust(cur1, 200);
        mm12.changeTrust(cur2, 200);
        mm23.changeTrust(cur2, 200);
        mm23.changeTrust(cur3, 200);
        mm34a.changeTrust(cur3, 200);
        mm34a.changeTrust(cur4, 200);
        mm34b.changeTrust(cur3, 200);
        mm34b.changeTrust(cur4, 200);
        destination.changeTrust(cur4, 200);

        gateway.pay(source, cur1, 80);
        gateway.pay(mm12, cur2, 40);
        gateway2.pay(mm23, cur3, 20);
        gateway2.pay(mm34a, cur4, 10);
        gateway2.pay(mm34b, cur4, 10);

        auto o1 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm12, {cur2, cur1, Price{2, 1}, 40});
        });
        auto o2 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm23, {cur3, cur2, Price{2, 1}, 20});
        });
        auto o3a = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm34a, {cur4, cur3, Price{2, 1}, 10});
        });
        auto o3b = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm34b, {cur4, cur3, Price{2, 1}, 10});
        });

        SECTION("missing selling line")
        {
            for_all_versions( *app, [&] {
                REQUIRE_THROWS_AS(mm34a.pay(gateway2, cur4, 10),
                                  ex_PAYMENT_UNDERFUNDED);
            });
        }

        SECTION("missing buying line")
        {
            for_all_versions(*app, [&] {
                REQUIRE_THROWS_AS(mm34a.changeTrust(cur3, 0),
                                  ex_CHANGE_TRUST_INVALID_LIMIT);
            });
        }
    }

    SECTION("path payment empty trust line for selling asset for offer for "
            "first exchange")
    {
        auto market = TestMarket{*app};
        auto source = root.create("source", minBalance4);
        auto destination = root.create("destination", minBalance1);
        auto mm12a = root.create("mm12a", minBalance3);
        auto mm12b = root.create("mm12b", minBalance3);
        auto mm23 = root.create("mm23", minBalance3);
        auto mm34 = root.create("mm34", minBalance3);

        source.changeTrust(cur1, 200);
        mm12a.changeTrust(cur1, 200);
        mm12a.changeTrust(cur2, 200);
        mm12b.changeTrust(cur1, 200);
        mm12b.changeTrust(cur2, 200);
        mm23.changeTrust(cur2, 200);
        mm23.changeTrust(cur3, 200);
        mm34.changeTrust(cur3, 200);
        mm34.changeTrust(cur4, 200);
        destination.changeTrust(cur4, 200);

        gateway.pay(source, cur1, 80);
        gateway.pay(mm12a, cur2, 40);
        gateway.pay(mm12b, cur2, 40);
        gateway2.pay(mm23, cur3, 20);
        gateway2.pay(mm34, cur4, 10);

        auto o1a = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm12a, {cur2, cur1, Price{2, 1}, 40});
        });
        auto o1b = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm12b, {cur2, cur1, Price{2, 1}, 40});
        });
        auto o2 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm23, {cur3, cur2, Price{2, 1}, 20});
        });
        auto o3 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm34, {cur4, cur3, Price{2, 1}, 10});
        });

        for_all_versions(*app, [&] {
            REQUIRE_THROWS_AS(mm12a.pay(gateway, cur2, 40),
                              ex_PAYMENT_UNDERFUNDED);
        });
    }

    SECTION("path payment empty trust line for selling asset for offer for "
            "second exchange")
    {
        auto market = TestMarket{*app};
        auto source = root.create("source", minBalance4);
        auto destination = root.create("destination", minBalance1);
        auto mm12 = root.create("mm12a", minBalance3);
        auto mm23a = root.create("mm23a", minBalance3);
        auto mm23b = root.create("mm23b", minBalance3);
        auto mm34 = root.create("mm34", minBalance3);

        source.changeTrust(cur1, 200);
        mm12.changeTrust(cur1, 200);
        mm12.changeTrust(cur2, 200);
        mm23a.changeTrust(cur2, 200);
        mm23a.changeTrust(cur3, 200);
        mm23b.changeTrust(cur2, 200);
        mm23b.changeTrust(cur3, 200);
        mm34.changeTrust(cur3, 200);
        mm34.changeTrust(cur4, 200);
        destination.changeTrust(cur4, 200);

        gateway.pay(source, cur1, 80);
        gateway.pay(mm12, cur2, 40);
        gateway2.pay(mm23a, cur3, 20);
        gateway2.pay(mm23b, cur3, 20);
        gateway2.pay(mm34, cur4, 10);

        auto o1 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm12, {cur2, cur1, Price{2, 1}, 40});
        });
        auto o2a = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm23a, {cur3, cur2, Price{2, 1}, 20});
        });
        auto o2b = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm23b, {cur3, cur2, Price{2, 1}, 20});
        });
        auto o3 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm34, {cur4, cur3, Price{2, 1}, 10});
        });

        

        for_all_versions(*app, [&] {
            REQUIRE_THROWS_AS(mm23a.pay(gateway2, cur3, 20),
                              ex_PAYMENT_UNDERFUNDED);
        });
    }

    SECTION("path payment empty trust line for selling asset for offer for "
            "last exchange")
    {
        auto market = TestMarket{*app};
        auto source = root.create("source", minBalance4);
        auto destination = root.create("destination", minBalance1);
        auto mm12 = root.create("mm12a", minBalance3);
        auto mm23 = root.create("mm23", minBalance3);
        auto mm34a = root.create("mm34a", minBalance3);
        auto mm34b = root.create("mm34b", minBalance3);

        source.changeTrust(cur1, 200);
        mm12.changeTrust(cur1, 200);
        mm12.changeTrust(cur2, 200);
        mm23.changeTrust(cur2, 200);
        mm23.changeTrust(cur3, 200);
        mm34a.changeTrust(cur3, 200);
        mm34a.changeTrust(cur4, 200);
        mm34b.changeTrust(cur3, 200);
        mm34b.changeTrust(cur4, 200);
        destination.changeTrust(cur4, 200);

        gateway.pay(source, cur1, 80);
        gateway.pay(mm12, cur2, 40);
        gateway2.pay(mm23, cur3, 20);
        gateway2.pay(mm34a, cur4, 10);
        gateway2.pay(mm34b, cur4, 10);

        auto o1 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm12, {cur2, cur1, Price{2, 1}, 40});
        });
        auto o2 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm23, {cur3, cur2, Price{2, 1}, 20});
        });
        auto o3a = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm34a, {cur4, cur3, Price{2, 1}, 10});
        });
        auto o3b = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm34b, {cur4, cur3, Price{2, 1}, 10});
        });

        for_all_versions(*app, [&] {
            REQUIRE_THROWS_AS(mm34a.pay(gateway2, cur4, 10),
                              ex_PAYMENT_UNDERFUNDED);
        });
    }

    SECTION("path payment full trust line for buying asset for offer for "
            "first exchange")
    {
        auto market = TestMarket{*app};
        auto source = root.create("source", minBalance4);
        auto destination = root.create("destination", minBalance1);
        auto mm12a = root.create("mm12a", minBalance3);
        auto mm12b = root.create("mm12b", minBalance3);
        auto mm23 = root.create("mm23", minBalance3);
        auto mm34 = root.create("mm34", minBalance3);

        source.changeTrust(cur1, 200);
        mm12a.changeTrust(cur1, 200);
        mm12a.changeTrust(cur2, 200);
        mm12b.changeTrust(cur1, 200);
        mm12b.changeTrust(cur2, 200);
        mm23.changeTrust(cur2, 200);
        mm23.changeTrust(cur3, 200);
        mm34.changeTrust(cur3, 200);
        mm34.changeTrust(cur4, 200);
        destination.changeTrust(cur4, 200);

        gateway.pay(source, cur1, 80);
        gateway.pay(mm12a, cur2, 40);
        gateway.pay(mm12b, cur2, 40);
        gateway2.pay(mm23, cur3, 20);
        gateway2.pay(mm34, cur4, 10);

        auto o1a = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm12a, {cur2, cur1, Price{2, 1}, 40});
        });
        auto o1b = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm12b, {cur2, cur1, Price{2, 1}, 40});
        });
        auto o2 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm23, {cur3, cur2, Price{2, 1}, 20});
        });
        auto o3 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm34, {cur4, cur3, Price{2, 1}, 10});
        });

        for_all_versions(*app, [&] {
            REQUIRE_THROWS_AS(gateway.pay(mm12a, cur1, 200),
                              ex_PAYMENT_LINE_FULL);
        });
    }

    SECTION("path payment full trust line for buying asset for offer for "
            "second exchange")
    {
        auto market = TestMarket{*app};
        auto source = root.create("source", minBalance4);
        auto destination = root.create("destination", minBalance1);
        auto mm12 = root.create("mm12a", minBalance3);
        auto mm23a = root.create("mm23a", minBalance3);
        auto mm23b = root.create("mm23b", minBalance3);
        auto mm34 = root.create("mm34", minBalance3);

        source.changeTrust(cur1, 200);
        mm12.changeTrust(cur1, 200);
        mm12.changeTrust(cur2, 200);
        mm23a.changeTrust(cur2, 200);
        mm23a.changeTrust(cur3, 200);
        mm23b.changeTrust(cur2, 200);
        mm23b.changeTrust(cur3, 200);
        mm34.changeTrust(cur3, 200);
        mm34.changeTrust(cur4, 200);
        destination.changeTrust(cur4, 200);

        gateway.pay(source, cur1, 80);
        gateway.pay(mm12, cur2, 40);
        gateway2.pay(mm23a, cur3, 20);
        gateway2.pay(mm23b, cur3, 20);
        gateway2.pay(mm34, cur4, 10);

        auto o1 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm12, {cur2, cur1, Price{2, 1}, 40});
        });
        auto o2a = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm23a, {cur3, cur2, Price{2, 1}, 20});
        });
        auto o2b = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm23b, {cur3, cur2, Price{2, 1}, 20});
        });
        auto o3 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm34, {cur4, cur3, Price{2, 1}, 10});
        });

        for_all_versions(*app, [&] {
            REQUIRE_THROWS_AS(gateway.pay(mm23a, cur2, 200),
                              ex_PAYMENT_LINE_FULL);
        });
    }

    SECTION("path payment full trust line for buying asset for offer for last "
            "exchange")
    {
        auto market = TestMarket{*app};
        auto source = root.create("source", minBalance4);
        auto destination = root.create("destination", minBalance1);
        auto mm12 = root.create("mm12a", minBalance3);
        auto mm23 = root.create("mm23", minBalance3);
        auto mm34a = root.create("mm34a", minBalance3);
        auto mm34b = root.create("mm34b", minBalance3);

        source.changeTrust(cur1, 200);
        mm12.changeTrust(cur1, 200);
        mm12.changeTrust(cur2, 200);
        mm23.changeTrust(cur2, 200);
        mm23.changeTrust(cur3, 200);
        mm34a.changeTrust(cur3, 200);
        mm34a.changeTrust(cur4, 200);
        mm34b.changeTrust(cur3, 200);
        mm34b.changeTrust(cur4, 200);
        destination.changeTrust(cur4, 200);

        gateway.pay(source, cur1, 80);
        gateway.pay(mm12, cur2, 40);
        gateway2.pay(mm23, cur3, 20);
        gateway2.pay(mm34a, cur4, 10);
        gateway2.pay(mm34b, cur4, 10);

        auto o1 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm12, {cur2, cur1, Price{2, 1}, 40});
        });
        auto o2 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm23, {cur3, cur2, Price{2, 1}, 20});
        });
        auto o3a = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm34a, {cur4, cur3, Price{2, 1}, 10});
        });
        auto o3b = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm34b, {cur4, cur3, Price{2, 1}, 10});
        });

        for_all_versions(*app, [&] {
            REQUIRE_THROWS_AS(gateway2.pay(mm34a, cur3, 200),
                              ex_PAYMENT_LINE_FULL);
        });
    }
    
    SECTION("path payment takes all offers, one offer per exchange")
    {
        auto market = TestMarket{*app};
        auto source = root.create("source", minBalance4);
        auto destination = root.create("destination", minBalance1);
        auto mm12 = root.create("mm12", minBalance3);
        auto mm23 = root.create("mm23", minBalance3);
        auto mm34 = root.create("mm34", minBalance3);

        source.changeTrust(cur1, 200);
        mm12.changeTrust(cur1, 200);
        mm12.changeTrust(cur2, 200);
        mm23.changeTrust(cur2, 200);
        mm23.changeTrust(cur3, 200);
        mm34.changeTrust(cur3, 200);
        mm34.changeTrust(cur4, 200);
        destination.changeTrust(cur4, 200);

        gateway.pay(source, cur1, 80);
        gateway.pay(mm12, cur2, 40);
        gateway2.pay(mm23, cur3, 20);
        gateway2.pay(mm34, cur4, 10);

        auto o1 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm12, {cur2, cur1, Price{2, 1}, 40});
        });
        auto o2 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm23, {cur3, cur2, Price{2, 1}, 20});
        });
        auto o3 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm34, {cur4, cur3, Price{2, 1}, 10});
        });

        for_all_versions(*app, [&] {
            auto actual = std::vector<ClaimOfferAtom>{};
            market.requireChanges(
                {{o1.key, OfferState::DELETED},
                 {o2.key, OfferState::DELETED},
                 {o3.key, OfferState::DELETED}},
                [&] {
                    actual = source
                                 .pathpay2(destination, cur1, 80, cur4, 10,
                                           {cur1, cur2, cur3, cur4})
                                 .success()
                                 .offers;
                });
            auto expected = std::vector<ClaimOfferAtom>{
                 o3.exchanged(10, 20),
                                                        o2.exchanged(20, 40),
                                                        o1.exchanged(40, 80)
                                                        };
            REQUIRE(actual == expected);
            // clang-format off
            market.requireBalances(
                {{source, {{xlm, minBalance4 - 2 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 0}, {cur4, 0}}},
                 {mm12, {{xlm, minBalance3 - 3 * txfee}, {cur1, 80}, {cur2, 0}, {cur3, 0}, {cur4, 0}}},
                 {mm23, {{xlm, minBalance3 - 3 * txfee}, {cur1, 0}, {cur2, 40}, {cur3, 0}, {cur4, 0}}},
                 {mm34, {{xlm, minBalance3 - 3 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 20}, {cur4, 0}}},
                 {destination, {{xlm, minBalance1 - txfee}, {cur1, 0}, {cur2, 0}, {cur3, 0}, {cur4, 10}}}});
            // clang-format on
        });
    }
    
    // step 5 is failing with: PATH_PAYMENT_TOO_FEW_OFFERS  
    SECTION("path payment uses all offers in a loop")
    {
        auto market = TestMarket{*app};
        auto source = root.create("source", minBalance4);
        auto destination = root.create("destination", minBalance1);
        auto mm12 = root.create("mm12", minBalance3);
        auto mm23 = root.create("mm23", minBalance3);
        auto mm34 = root.create("mm34", minBalance3);
        auto mm41 = root.create("mm41", minBalance3);

        source.changeTrust(cur1, 16000000);
        mm12.changeTrust(cur1, 16000000);
        mm12.changeTrust(cur2, 16000000);
        mm23.changeTrust(cur2, 16000000);
        mm23.changeTrust(cur3, 16000000);
        mm34.changeTrust(cur3, 16000000);
        mm34.changeTrust(cur4, 16000000);
        mm41.changeTrust(cur4, 16000000);
        mm41.changeTrust(cur1, 16000000);
        destination.changeTrust(cur4, 16000000);

        gateway.pay(source, cur1, 8000000);
        gateway.pay(mm12, cur2, 8000000);
        gateway2.pay(mm23, cur3, 8000000);
        gateway2.pay(mm34, cur4, 8000000);
        gateway.pay(mm41, cur1, 8000000);

        auto o1 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm12, {cur2, cur1, Price{2, 1}, 1062501});
        });
        auto o2 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm23, {cur3, cur2, Price{2, 1}, 1000000});
        });
        auto o3 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm34, {cur4, cur3, Price{2, 1}, 1000000});
        });
        auto o4 = market.requireChangesWithOffer({}, [&] {
            return market.addOffer(mm41, {cur1, cur4, Price{2, 1}, 1000000});
        });

        for_all_versions(*app, [&] {
            auto actual = std::vector<ClaimOfferAtom>{};
            market.requireChanges(
                {{o1.key, {cur2, cur1, Price{2, 1}, 1}},
                 {o2.key, {cur3, cur2, Price{2, 1}, 468750}},
                 {o3.key, {cur4, cur3, Price{2, 1}, 734375}},
                 {o4.key, {cur1, cur4, Price{2, 1}, 875000}}},
                [&] {
                    actual = source
                                 .pathpay2(destination, cur1, 2000000, cur4, 0,
                                           {cur2, cur3, cur4, cur1, cur2,
                                            cur3 })
                                 .success()
                                 .offers;
                });
            auto expected = std::vector<ClaimOfferAtom>{
                o3.exchanged(15625, 31250),     
                o2.exchanged(31250, 62500),
                o1.exchanged(62500, 125000),    
                o4.exchanged(125000, 250000),
                o3.exchanged(250000, 500000),   
                o2.exchanged(500000, 1000000),
                o1.exchanged(1000000, 2000000)};

            REQUIRE(actual == expected);
            // clang-format off
            market.requireBalances(
                {{source, {{xlm, minBalance4 - 2 * txfee}, {cur1, 6000000}, {cur2, 0}, {cur3, 0}, {cur4, 0}}},
                 {mm12, {{xlm, minBalance3 - 3 * txfee}, {cur1, 2125000}, {cur2, 6937500}, {cur3, 0}, {cur4, 0}}},
                 {mm23, {{xlm, minBalance3 - 3 * txfee}, {cur1, 0}, {cur2, 1062500}, {cur3, 7468750}, {cur4, 0}}},
                 {mm34, {{xlm, minBalance3 - 3 * txfee}, {cur1, 0}, {cur2, 0}, {cur3, 531250}, {cur4, 7734375}}},
                 {mm41, {{xlm, minBalance3 - 3 * txfee}, {cur1, 7875000}, {cur2, 0}, {cur3, 0}, {cur4, 250000}}},
                 {destination, {{xlm, minBalance1 - txfee}, {cur1, 0}, {cur2, 0}, {cur3, 0}, {cur4, 15625}}}});
            // clang-format on
        });
    }
    
    
    
    /* TODOJED:
    SECTION("path payment with cycle")
    {
        for_all_versions(*app, [&] {
            // Create 3 different cycles.
            // First cycle involves 3 transaction in which
            // buying price is
            // always half - so sender buys 8 times as much XLM
            // as he/she
            // sells (arbitrage).
            // Second cycle involves 3 transaction in which
            // buying price is
            // always two - so sender buys 8 times as much XLM
            // as he/she
            // sells (anti-arbitrage).
            // Thanks to send max option this transaction is
            // rejected.
            // Third cycle is similar to second, but send max is
            // set to a
            // high value, so transaction proceeds even if it
            // makes sender
            // lose a lot of XLM.

            // Each cycle is created in 3 variants (to check if
            // behavior
            // does not depend of nativeness of asset):
            // * XLM -> USD -> IDR -> XLM
            // * USD -> IDR -> XLM -> USD
            // * IDR -> XLM -> USD -> IDR
            // To create variants, rotateRight() function is
            // used on
            // accounts, offers and assets -
            // it greatly simplified index calculation in the
            // code.

            auto market = TestMarket{*app};
            auto paymentAmount = int64_t{100000000}; // amount of money that
                                                     // 'destination'
                                                     // account will receive
            auto offerAmount = 8 * paymentAmount;    // amount of money in
            // offer required to pass
            // - needs 8x of payment
            // for anti-arbitrage case
            auto initialBalance = 2 * offerAmount; // we need twice as much
                                                   // money as in the
            // offer because of Price{2, 1} that is
            // used in one case
            auto txFee = app->getLedgerManager().getLastTxFee();

            auto assets = std::deque<Asset>{xlm, usd, idr};
            int pathSize = (int)assets.size();
            auto accounts = std::deque<TestAccount>{};

            auto setupAccount = [&](const std::string& name) {
                // setup account with required trustlines and
                // money both in
                // native and assets

                auto account = root.create(name, initialBalance);
                account.changeTrust(idr, trustLineLimit);
                gateway.pay(account, idr, initialBalance);
                account.changeTrust(usd, trustLineLimit);
                gateway2.pay(account, usd, initialBalance);

                return account;
            };

            auto validateAccountAsset = [&](const TestAccount& account,
                                            int assetIndex, int64_t difference,
                                            int feeCount) {
                if (assets[assetIndex].type() == ASSET_TYPE_NATIVE)
                {
                    REQUIRE(account.getBalance() ==
                            initialBalance + difference - feeCount * txFee);
                }
                else
                {
                    REQUIRE(account.loadTrustLine(assets[assetIndex]).balance ==
                            initialBalance + difference);
                }
            };
            auto validateAccountAssets = [&](const TestAccount& account,
                                             int assetIndex, int64_t difference,
                                             int feeCount) {
                for (int i = 0; i < pathSize; i++)
                {
                    validateAccountAsset(account, i,
                                         (assetIndex == i) ? difference : 0,
                                         feeCount);
                }
            };
            auto validateOffer = [&](const TestAccount& account,
                                     int64_t offerId, int64_t difference) {
                LedgerTxn ltx(app->getLedgerTxnRoot());
                auto offer =
                    stellar::loadOffer(ltx, account.getPublicKey(), offerId);
                auto const& oe = offer.current().data.offer();
                REQUIRE(oe.amount == offerAmount + difference);
            };

            auto source = setupAccount("S");
            auto destination = setupAccount("D");

            auto validateSource = [&](int64_t difference) {
                validateAccountAssets(source, 0, difference, 3);
            };
            auto validateDestination = [&](int64_t difference) {
                validateAccountAssets(destination, 0, difference, 2);
            };

            for (int i = 0; i < pathSize;
                 i++) // create account for each known asset
            {
                accounts.emplace_back(
                    setupAccount(std::string{"C"} + std::to_string(i)));
                validateAccountAssets(accounts[i], 0, 0,
                                      2); // 2x change trust called
            }

            auto testPath = [&](const std::string& name, const Price& price,
                                int maxMultipler, bool overSendMax) {
                SECTION(name)
                {
                    auto offers = std::deque<int64_t>{};
                    for (int i = 0; i < pathSize; i++)
                    {
                        offers.push_back(
                            market
                                .requireChangesWithOffer(
                                    {},
                                    [&] {
                                        return market.addOffer(
                                            accounts[i],
                                            {assets[i],
                                             assets[(i + 2) % pathSize], price,
                                             offerAmount});
                                    })
                                .key.offerID);
                        validateOffer(accounts[i], offers[i], 0);
                    }

                    for (int i = 0; i < pathSize; i++)
                    {
                        auto path = std::vector<Asset>{assets[1], assets[2]};
                        SECTION(std::string{"send with path ("} +
                                assetPathToString(assets) + ")")
                        {
                            auto destinationMultiplier = overSendMax ? 0 : 1;
                            auto sellerMultipler =
                                overSendMax ? Price{0, 1} : Price{1, 1};
                            auto buyerMultipler = sellerMultipler * price;

                            if (overSendMax)
                                REQUIRE_THROWS_AS(
                                    source.pathpay2(
                                        destination, assets[0],
                                        maxMultipler * paymentAmount, assets[0],
                                        paymentAmount, path),
                                    ex_PATH_PAYMENT_OVER_SENDMAX);
                            else
                                source.pathpay2(destination, assets[0],
                                                maxMultipler * paymentAmount,
                                                assets[0], paymentAmount, path);

                            for (int j = 0; j < pathSize; j++)
                            {
                                auto index = (pathSize - j) %
                                             pathSize; // it is done from
                                                       // end of path to
                                                       // begin of path
                                validateAccountAsset(accounts[index], index,
                                                     -paymentAmount *
                                                         sellerMultipler,
                                                     3); // sold asset
                                validateOffer(accounts[index], offers[index],
                                              -paymentAmount *
                                                  sellerMultipler); // sold
                                                                    // asset
                                validateAccountAsset(
                                    accounts[index], (index + 2) % pathSize,
                                    paymentAmount * buyerMultipler,
                                    3); // bought asset
                                validateAccountAsset(accounts[index],
                                                     (index + 1) % pathSize, 0,
                                                     3); // ignored asset
                                sellerMultipler = sellerMultipler * price;
                                buyerMultipler = buyerMultipler * price;
                            }

                            validateSource(-paymentAmount * sellerMultipler);
                            validateDestination(paymentAmount *
                                                destinationMultiplier);
                        }

                        // next cycle variant
                        rotateRight(assets);
                        rotateRight(accounts);
                        rotateRight(offers);
                    }
                }
            };

            // cycle with every asset on path costing half as
            // much as
            // previous - 8 times gain
            testPath("arbitrage", Price(1, 2), 1, false);
            // cycle with every asset on path costing twice as
            // much as
            // previous - 8 times loss - unacceptable
            testPath("anti-arbitrage", Price(2, 1), 1, true);
            // cycle with every asset on path costing twice as
            // much as
            // previous - 8 times loss - acceptable (but not
            // wise to do)
            testPath("anti-arbitrage with big sendmax", Price(2, 1), 8, false);
        });
    }
    */

    // market.addOffer(mm, {selling, buying, Price{buy, sell}, amountSelling});
    // Test where:
    //   there is dust left for the sender
    //   there is dust left in an inner step (What do we do here? Pull from last offer)
    //   there is dust left for the sender when there are multiple orders
    //   there is dust left in an inner step when there are multiple orders
    //   TODOJED: there is XLM dust for the sender
    //   TODOJED: there is XLM dust for an inner step
    SECTION("path payment rounding")
    {
        // Dust of sender goes back to the issuer

        auto source =
            root.create("source", app->getLedgerManager().getLastMinBalance(1) +
                                      10 * txfee);
        auto mm = root.create(
            "mm", app->getLedgerManager().getLastMinBalance(4) + 10 * txfee);
        
        auto mm2 = root.create(
            "mm2", app->getLedgerManager().getLastMinBalance(4) + 10 * txfee);

        auto destination = root.create(
            "destination",
            app->getLedgerManager().getLastMinBalance(1) + 10 * txfee);
        
        SECTION("dust left for the sender")
        {
            source.changeTrust(cur1, 1000);
            mm.changeTrust(cur1, 10000);
            mm.changeTrust(cur2, 20000);
            destination.changeTrust(cur2, 1001);

            gateway.pay(source, cur1, 1000);
            gateway.pay(mm, cur2, 10000);

            auto market = TestMarket{*app};
            auto o1 = market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm, {cur2, cur1, Price{10, 2}, 100});
            });

            for_all_versions(*app, [&] {
                market.requireChanges(
                    {{o1.key, {cur2, cur1, Price{10, 2}, 99}}}, [&] {
                        source.pathpay2(destination, cur1, 9, cur2, 0, {});
                    });
                market.requireBalances({{source, {{cur1, 991}}},
                                        {mm, {{cur1, 5}, {cur2, 9999}}},
                                        {destination, {{cur2, 1}}}});
            });
        }        
        
        SECTION("dust left for an inner step")
        {
            source.changeTrust(cur1, 9);
            mm.changeTrust(cur1, 10000);
            mm.changeTrust(cur2, 20000);

            mm2.changeTrust(cur2, 10000);
            mm2.changeTrust(cur3, 20000);

            destination.changeTrust(cur3, 1001);

            gateway.pay(source, cur1, 9);
            gateway.pay(mm, cur2, 10000);
            gateway2.pay(mm2, cur3, 10000);

            auto market = TestMarket{*app};
            auto o1 = market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm, {cur2, cur1, Price{1, 1}, 100});
            });
            auto o2 = market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm2, {cur3, cur2, Price{10, 2}, 100});
            });

            for_all_versions(*app, [&] {
                market.requireChanges(
                    {{o1.key, {cur2, cur1, Price{1, 1}, 91}},
                     {o2.key, {cur3, cur2, Price{10, 2}, 99}}},
                    [&] {
                        source.pathpay2(destination, cur1, 9, cur3, 0, {cur2});
                    });
                market.requireBalances({{source, {{cur1, 0}}},
                                        {mm, {{cur1, 9}, {cur2, 9991}}},
                                        {mm2, {{cur2, 5}, {cur3, 9999}}},
                                        {destination, {{cur3, 1}}}});
            });
        }
        
        SECTION("XLM dust left for the sender")
        {
            auto source2 = root.create(
                "source2", minBalance + txfee + 1000);

            mm.changeTrust(cur2, 20000);
            destination.changeTrust(cur2, 1001);

            gateway.pay(mm, cur2, 10000);

            auto market = TestMarket{*app};
            auto o1 = market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm, {cur2, xlm, Price{10, 2}, 100});
            });

            for_all_versions(*app, [&] {
                market.requireChanges(
                    {{o1.key, {cur2, xlm, Price{10, 2}, 99}}}, [&] {
                        source2.pathpay2(destination, xlm, 9, cur2, 0, {});
                    });
                market.requireBalances({{source2, {{xlm, minBalance + 995}}},
                                        {mm, {{xlm, 5}, {cur2, 9999}}},
                                        {destination, {{cur2, 1}}}});
            });
        }
    }

    

    SECTION("liabilities")
    {
        SECTION("cannot pay balance below selling liabilities")
        {
            TestMarket market(*app);
            auto source = root.create("source", minBalance2);
            auto destination = root.create("destination", minBalance2);
            auto mm12 = root.create("mm12", minBalance3);

            source.changeTrust(cur1, 200);
            mm12.changeTrust(cur1, 200);
            mm12.changeTrust(cur2, 200);
            destination.changeTrust(cur2, 200);

            gateway.pay(source, cur1, 100);
            gateway.pay(mm12, cur2, 100);

            auto offer = market.requireChangesWithOffer({}, [&] {
                return market.addOffer(source, {cur1, xlm, Price{1, 1}, 50});
            });
            auto o2 = market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm12, {cur2, cur1, Price{1, 1}, 100});
            });

           
            for_all_versions(*app, [&] {
                REQUIRE_THROWS_AS(source.pathpay2(destination, cur1, 51, cur2,
                                                  51, {cur1, cur2}),
                                  ex_PATH_PAYMENT_UNDERFUNDED);
                source.pathpay2(destination, cur1, 50, cur2, 50, {cur1, cur2});
            });
        }

        SECTION("cannot receive such that balance + buying liabilities exceeds"
                " limit")
        {
            TestMarket market(*app);
            auto source = root.create("source", minBalance2);
            auto destination = root.create("destination", minBalance2);
            auto mm12 = root.create("mm12", minBalance3);

            source.changeTrust(cur1, 200);
            mm12.changeTrust(cur1, 200);
            mm12.changeTrust(cur2, 200);
            destination.changeTrust(cur2, 200);

            gateway.pay(source, cur1, 100);
            gateway.pay(mm12, cur2, 100);
            gateway.pay(destination, cur2, 100);

            auto offer = market.requireChangesWithOffer({}, [&] {
                return market.addOffer(destination,
                                       {xlm, cur2, Price{1, 1}, 50});
            });
            auto o2 = market.requireChangesWithOffer({}, [&] {
                return market.addOffer(mm12, {cur2, cur1, Price{1, 1}, 100});
            });

           
            for_all_versions(*app, [&] {
                REQUIRE_THROWS_AS(source.pathpay2(destination, cur1, 51, cur2,
                                                  51, {cur1, cur2}),
                                  ex_PATH_PAYMENT_LINE_FULL);
                source.pathpay2(destination, cur1, 50, cur2, 50, {cur1, cur2});
            });
        }
    }
}
