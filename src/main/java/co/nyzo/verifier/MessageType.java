package co.nyzo.verifier;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public enum MessageType {

    // standard-operation messages
    Invalid0(0),
    // BootstrapRequest1(1), replaced with BootstrapRequestV2_35
    // BootstrapResponse2(2), replaced with BootstrapResponseV2_36
    // NodeJoin3(3), replaced with NodeJoinV2_43
    // NodeJoinResponse4(4), replaced with NodeJoinResponseV2_44
    Transaction5(5),
    TransactionResponse6(6),
    PreviousHashRequest7(7),
    PreviousHashResponse8(8),
    NewBlock9(9),
    NewBlockResponse10(10),
    BlockRequest11(11),
    BlockResponse12(12),
    TransactionPoolRequest13(13),
    TransactionPoolResponse14(14),
    MeshRequest15(15),
    MeshResponse16(16),
    StatusRequest17(17),
    StatusResponse18(18),
    BlockVote19(19),
    BlockVoteResponse20(20),
    NewVerifierVote21(21),
    NewVerifierVoteResponse22(22),
    MissingBlockVoteRequest23(23),
    MissingBlockVoteResponse24(24),
    MissingBlockRequest25(25),
    MissingBlockResponse26(26),
    TimestampRequest27(27),
    TimestampResponse28(28),
    HashVoteOverrideRequest29(29),
    HashVoteOverrideResponse30(30),
    ConsensusThresholdOverrideRequest31(31),
    ConsensusThresholdOverrideResponse32(32),
    NewVerifierVoteOverrideRequest33(33),
    NewVerifierVoteOverrideResponse34(34),
    BootstrapRequestV2_35(35),
    BootstrapResponseV2_36(36),
    BlockWithVotesRequest37(37),
    BlockWithVotesResponse38(38),
    VerifierRemovalVote39(39),
    VerifierRemovalVoteResponse40(40),
    FullMeshRequest41(41),
    FullMeshResponse42(42),
    NodeJoinV2_43(43),
    NodeJoinResponseV2_44(44),
    FrozenEdgeBalanceListRequest45(45),
    FrozenEdgeBalanceListResponse46(46),
    // CycleTransactionSignature47(47),  // not used after blockchain v1
    // CycleTransactionSignatureResponse48(48),  // not used after blockchain v1
    // CycleTransactionListRequest49(49),  // not used after blockchain v1
    // CycleTransactionListResponse50(50),  // not used after blockchain v1
    MinimalBlock51(51),
    MinimalBlockResponse52(52),   // currently unused -- UDP-only message
    IpAddressRequest53(53),
    IpAddressResponse54(54),
    VersionRequest55(55),
    VersionResponse56(56),

    // test messages
    Ping200(200),
    PingResponse201(201),

    // maintenance messages
    UpdateRequest300(300),  // updates the verifier with the latest code from the Git repository, rebuilds, and restarts
    UpdateResponse301(301),

    // Debugging and private messages.
    BlockRejectionRequest400(400),  // discards all blocks received for the next 10 seconds
    BlockRejectionResponse401(401),
    DetachmentRequest402(402),  // stops producing blocks for two verifier cycles
    DetachmentResponse403(403),
    UnfrozenBlockPoolPurgeRequest404(404),  // clears the unfrozen block pool
    UnfrozenBlockPoolPurgeResponse405(405),
    UnfrozenBlockPoolStatusRequest406(406),  // gets textual information about the unfrozen block pool
    UnfrozenBlockPoolStatusResponse407(407),
    MeshStatusRequest408(408),  // gets textual information about the mesh
    MeshStatusResponse409(409),
    TogglePauseRequest410(410),  // pauses/un-pauses verifier
    TogglePauseResponse411(411),
    ConsensusTallyStatusRequest412(412),
    ConsensusTallyStatusResponse413(413),
    NewVerifierTallyStatusRequest414(414),
    NewVerifierTallyStatusResponse415(415),
    BlacklistStatusRequest416(416),
    BlacklistStatusResponse417(417),
    PerformanceScoreStatusRequest418(418),
    PerformanceScoreStatusResponse419(419),
    VerifierRemovalTallyStatusRequest420(420),
    VerifierRemovalTallyStatusResponse421(421),
    BlockDelayRequest422(422),
    BlockDelayResponse423(423),
    WhitelistRequest424(424),
    WhitelistResponse425(425),

    // bootstrapping messages
    ResetRequest500(500),   // resets the blockchain
    ResetResponse501(501),

    // the highest allowable message number is 65535
    IncomingRequest65533(65533),  // for debugging -- passed to readFromStream by meshListener/meshListenerController
    Error65534(65534),
    Unknown65535(65535);

    private static final Map<Integer, MessageType> messageTypeMap = new ConcurrentHashMap<>();
    static {
        for (MessageType messageType : values()) {
            messageTypeMap.put(messageType.getValue(), messageType);
        }
    }

    private int value;

    MessageType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static MessageType forValue(int value) {
        return messageTypeMap.getOrDefault(value, Unknown65535);
    }
}
