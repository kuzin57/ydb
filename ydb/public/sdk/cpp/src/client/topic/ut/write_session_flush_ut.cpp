#include "ut_utils/topic_sdk_test_setup.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::inline Dev::NTopic::NTests {
namespace {

TContinuationToken WaitForWriteToken(IWriteSession& session) {
    while (true) {
        UNIT_ASSERT_C(session.WaitEvent().Wait(TDuration::Seconds(30)), "timeout waiting for write token");
        for (auto& event : session.GetEvents()) {
            if (auto* ready = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&event)) {
                return std::move(ready->ContinuationToken);
            }
            if (auto* closed = std::get_if<TSessionClosedEvent>(&event)) {
                UNIT_FAIL("write session closed unexpectedly: " << closed->GetIssues().ToString());
            }
        }
    }
}

void CheckBatchAcknowledgements(const std::string& testName, ECodec codec, bool autoSeqNo) {
    auto serverSettings = TTopicSdkTestSetup::MakeServerSettings();
    serverSettings.FeatureFlags.SetEnableTopicMessagesBatching(true);
    serverSettings.FeatureFlags.SetEnableTopicWriteOffsetDeltaInKeys(true);
    TTopicSdkTestSetup setup(testName, serverSettings);
    auto client = setup.MakeClient();

    // A nonzero initial sequence number also checks conversion from internal IDs
    // back to the public sequence numbers in automatically numbered sessions.
    setup.Write("seed", 0, std::string(TEST_MESSAGE_GROUP_ID), 100);
    uint64_t nextSeqNo = 101;
    uint64_t nextOffset = 1;
    const std::string payload(16 * 1024, 'x');
    for (size_t messageCount : {size_t{1}, size_t{2}, size_t{128}}) {
        auto session = client.CreateWriteSession(
            TWriteSessionSettings()
                .Path(setup.GetTopicPath())
                .ProducerId(TEST_MESSAGE_GROUP_ID)
                .MessageGroupId(TEST_MESSAGE_GROUP_ID)
                .PartitionId(0)
                .Codec(codec)
                .BatchFlushMessageCount(129)
                .BatchFlushInterval(TDuration::Hours(1)));

        size_t acknowledged = 0;
        std::optional<TContinuationToken> token;
        auto collectEvents = [&] {
            UNIT_ASSERT_C(session->WaitEvent().Wait(TDuration::Seconds(30)), "timeout waiting for write events");
            for (auto& event : session->GetEvents()) {
                if (const auto* acks = std::get_if<TWriteSessionEvent::TAcksEvent>(&event)) {
                    for (const auto& ack : acks->Acks) {
                        UNIT_ASSERT(acknowledged < messageCount);
                        UNIT_ASSERT_VALUES_EQUAL(ack.SeqNo, nextSeqNo + acknowledged);
                        UNIT_ASSERT(ack.State == TWriteSessionEvent::TWriteAck::EEventState::EES_WRITTEN);
                        UNIT_ASSERT(ack.Details);
                        UNIT_ASSERT_VALUES_EQUAL(ack.Details->Offset, nextOffset + acknowledged);
                        ++acknowledged;
                    }
                } else if (auto* ready = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&event)) {
                    token = std::move(ready->ContinuationToken);
                } else if (const auto* closed = std::get_if<TSessionClosedEvent>(&event)) {
                    UNIT_FAIL("write session closed unexpectedly: " << closed->GetIssues().ToString());
                }
            }
        };
        for (size_t i = 0; i < messageCount; ++i) {
            while (!token) {
                collectEvents();
            }
            TWriteMessage message(payload);
            if (!autoSeqNo) {
                message.SeqNo(nextSeqNo + i);
            }
            session->Write(std::move(*token), std::move(message));
            token.reset();
        }
        // Explicit flush makes the test independent of the batching timer.
        auto flushed = session->Flush();
        UNIT_ASSERT_C(flushed.Wait(TDuration::Seconds(30)), "batch flush timed out");
        UNIT_ASSERT(flushed.GetValue());
        while (acknowledged < messageCount) {
            collectEvents();
        }

        const auto counters = session->GetCounters();
        UNIT_ASSERT_VALUES_EQUAL(counters->MessagesWritten->Val(), messageCount);
        UNIT_ASSERT_VALUES_EQUAL(counters->BytesWritten->Val(), messageCount * payload.size());
        UNIT_ASSERT_VALUES_EQUAL(counters->MessagesInflight->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightUncompressed->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightCompressed->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightTotal->Val(), 0);
        UNIT_ASSERT(session->Close(TDuration::Zero()));
        nextSeqNo += messageCount;
        nextOffset += messageCount;
    }
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(WriteSessionFlush) {
    Y_UNIT_TEST(GzipBatchAcknowledgements) {
        CheckBatchAcknowledgements(TEST_CASE_NAME, ECodec::GZIP, false);
    }

    Y_UNIT_TEST(GzipBatchAcknowledgementsWithAutoSeqNo) {
        CheckBatchAcknowledgements(TEST_CASE_NAME, ECodec::GZIP, true);
    }

    Y_UNIT_TEST(ZstdBatchAcknowledgements) {
        CheckBatchAcknowledgements(TEST_CASE_NAME, ECodec::ZSTD, false);
    }

    Y_UNIT_TEST(RawBatchAcknowledgements) {
        CheckBatchAcknowledgements(TEST_CASE_NAME, ECodec::RAW, false);
    }

    Y_UNIT_TEST(KafkaBatchAcknowledgementsWithAutoSeqNo) {
        CheckBatchAcknowledgements(TEST_CASE_NAME, ECodec::KAFKA_BATCH, true);
    }

    Y_UNIT_TEST(CloseImmediatelyAfterFlush) {
        TTopicSdkTestSetup setup(TEST_CASE_NAME);
        auto client = setup.MakeClient();
        auto session = client.CreateWriteSession(
            TWriteSessionSettings()
                .Path(setup.GetTopicPath())
                .MessageGroupId(TEST_MESSAGE_GROUP_ID)
                .Codec(ECodec::RAW)
                .BatchFlushMessageCount(10)
                .BatchFlushInterval(TDuration::Hours(1)));

        session->Write(WaitForWriteToken(*session), "message");

        auto firstFlush = session->Flush();
        auto secondFlush = session->Flush();
        auto reentrantFlushPromise = NThreading::NewPromise<bool>();
        auto reentrantFlush = reentrantFlushPromise.GetFuture();
        firstFlush.Subscribe([session, reentrantFlushPromise](const NThreading::TFuture<bool>& result) mutable {
            reentrantFlushPromise.TrySetValue(result.GetValue() && session->Flush().GetValueSync());
        });

        UNIT_ASSERT_C(firstFlush.Wait(TDuration::Seconds(30)), "first flush timed out");
        UNIT_ASSERT_C(secondFlush.Wait(TDuration::Seconds(30)), "second flush timed out");
        UNIT_ASSERT_C(reentrantFlush.Wait(TDuration::Seconds(30)), "reentrant flush timed out");
        UNIT_ASSERT(firstFlush.GetValue());
        UNIT_ASSERT(secondFlush.GetValue());
        UNIT_ASSERT(reentrantFlush.GetValue());
        UNIT_ASSERT(session->Close(TDuration::Zero()));
    }
}

} // namespace NYdb::NTopic::NTests
