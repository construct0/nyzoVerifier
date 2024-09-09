package co.nyzo.verifier.client.commands;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import co.nyzo.verifier.ApprovedCycleTransaction;
import co.nyzo.verifier.BalanceList;
import co.nyzo.verifier.BalanceListItem;
import co.nyzo.verifier.BalanceListManager;
import co.nyzo.verifier.Block;
import co.nyzo.verifier.BlockManager;
import co.nyzo.verifier.ByteUtil;
import co.nyzo.verifier.HistoricalBlockManager;
import co.nyzo.verifier.Transaction;
import co.nyzo.verifier.client.CommandOutput;
import co.nyzo.verifier.client.CommandTable;
import co.nyzo.verifier.client.CommandTableHeader;
import co.nyzo.verifier.client.ExecutionResult;
import co.nyzo.verifier.client.SimpleExecutionResult;
import co.nyzo.verifier.client.ValidationResult;
import co.nyzo.verifier.nyzoString.NyzoStringEncoder;
import co.nyzo.verifier.nyzoString.NyzoStringPublicIdentifier;
import co.nyzo.verifier.nyzoString.NyzoStringSignature;
import co.nyzo.verifier.util.PrintUtil;
import co.nyzo.verifier.web.WebUtil;

public class BalanceListCommand implements Command {
     @Override
    public String getShortCommand(){
        return "BL";
    }

    @Override
    public String getLongCommand(){
        return "balanceList";
    }

    @Override
    public String getDescription(){
        return "search for a balance list";
    }

    @Override
    public String[] getArgumentNames(){
        return new String[] { "block height (optional)" };
    }

    @Override
    public String[] getArgumentIdentifiers(){
        return new String[] { "blockHeight" };
    }

    @Override
    public boolean requiresValidation(){
        return false;
    }

    @Override
    public boolean requiresConfirmation() {
        return false;
    }

    @Override
    public boolean isLongRunning() {
        return false;
    }

    @Override
    public ValidationResult validate(List<String> argumentValues, CommandOutput output) {
        return null;
    }

    @Override
    public ExecutionResult run(List<String> args, CommandOutput output){
        List<String> notices = new ArrayList<>();
        List<String> errors = new ArrayList<>();

        long blockHeight = -1L;
        Block frozenEdge = BlockManager.getFrozenEdge();
        long frozenEdgeHeight = frozenEdge.getBlockHeight();

        try {
            blockHeight = Long.parseLong(args.get(0));
        } catch (Exception ignored) {}

        long minimumTimestamp = -1L;
        long maximumTimestamp = -1L;

        if(blockHeight >= 0){
            notices.add("Using block height of " + blockHeight + " for search");
        } else {
            notices.add("Using block height of " + frozenEdgeHeight + " for search");
            blockHeight = frozenEdgeHeight;
        }

        minimumTimestamp = BlockManager.startTimestampForHeight(blockHeight);
        maximumTimestamp = BlockManager.startTimestampForHeight(blockHeight + 1L) - 1L;

        CommandTable parentTable = new CommandTable(
            new CommandTableHeader("type", "type"),
            new CommandTableHeader("hash", "hash"),
            new CommandTableHeader("height", "height"),
            new CommandTableHeader("blockchain version", "blockchainVersion"),
            new CommandTableHeader("rollover fees to next block", "rolloverFees"),
            new CommandTableHeader("total coins in system", "totalCoins"),
            new CommandTableHeader("unlock threshold", "unlockThreshold"),
            new CommandTableHeader("unlock transfer sum", "unlockTransferSum"),
            new CommandTableHeader("available unlock amount", "availableUnlockAmount")
        );

        CommandTable dependantBalanceTable = new CommandTable(
            new CommandTableHeader("type", "type"),
            new CommandTableHeader("identifier", "identifier"),
            new CommandTableHeader("identifier nyzo string", "identifierNyzoString"),
            new CommandTableHeader("balance", "balance"),
            new CommandTableHeader("blocks until fee", "blocksUntilFee")
        );

        CommandTable dependantPreviousVerifiersTable = new CommandTable(
            new CommandTableHeader("type", "type"),
            new CommandTableHeader("identifier", "identifier"),
            new CommandTableHeader("identifier nyzo string", "identifierNyzoString")
        );

        CommandTable dependantPendingCycleTransactionsTable = new CommandTable(
            new CommandTableHeader("type", "type"),
            new CommandTableHeader("initiator identifier", "initiatorIdentifier", true),
            new CommandTableHeader("initiator identifier nyzo string", "initiatorIdentifierNyzoString", true),
            new CommandTableHeader("receiver identifier", "receiverIdentifier", true),
            new CommandTableHeader("receiver identifier nyzo string", "receiverIdentifierNyzoString", true),
            new CommandTableHeader("amount", "amount"),
            new CommandTableHeader("height", "height"),
            new CommandTableHeader("initiator data", "initiatorData", true),
            new CommandTableHeader("# votes", "numberOfVotes"),
            new CommandTableHeader("# yes votes", "numberOfYesVotes"),
            new CommandTableHeader("signature", "signature", true),
            new CommandTableHeader("signature nyzo string", "signatureNyzoString", true)
        );

        CommandTable dependantRecentlyApprovedCycleTransactionsTable = new CommandTable(
            new CommandTableHeader("type", "type"),
            new CommandTableHeader("initiator identifier", "initiatorIdentifier"),
            new CommandTableHeader("initiator identifier nyzo string", "initiatorIdentifierNyzoString"),
            new CommandTableHeader("receiver identifier", "receiverIdentifier"),
            new CommandTableHeader("receiver identifier nyzo string", "receiverIdentifierNyzoString"),
            new CommandTableHeader("approval height", "approvalHeight"),
            new CommandTableHeader("amount", "amount") 
        );

        if(minimumTimestamp > 0){
            BalanceList balanceList = null;

            if(frozenEdgeHeight == blockHeight){
                balanceList = BalanceListManager.getFrozenEdgeList();
            } else {
                balanceList = BlockManager.loadBalanceListFromFileForHeight(blockHeight);

                if(balanceList != null){
                    notices.add("This result is a historical representation");
                }
            }

            if(balanceList == null){
                errors.add("balance list is null");
            } else {
                parentTable.addRow(
                    "metadata",
                    ByteUtil.arrayAsStringWithDashes(balanceList.getHash()),
                    balanceList.getBlockHeight(),
                    balanceList.getBlockchainVersion(),
                    PrintUtil.printAmountAsMicronyzos(balanceList.getRolloverFees()),
                    PrintUtil.printAmountWithCommas(Transaction.micronyzosInSystem),
                    PrintUtil.printAmountWithCommas(balanceList.getUnlockThreshold()),
                    PrintUtil.printAmountWithCommas(balanceList.getUnlockTransferSum()),
                    PrintUtil.printAmountWithCommas(balanceList.getUnlockThreshold() - balanceList.getUnlockTransferSum())
                );


                List<BalanceListItem> items = balanceList.getItems();
                for(BalanceListItem item : items){
                    dependantBalanceTable.addRow(
                        "balance",
                        ByteUtil.arrayAsStringWithDashes(item.getIdentifier()),
                        NyzoStringEncoder.encode(new NyzoStringPublicIdentifier(item.getIdentifier())),
                        PrintUtil.printAmountWithCommas(item.getBalance()),
                        item.getBlocksUntilFee()
                    );
                }


                List<byte[]> previousVerifiers = balanceList.getPreviousVerifiers();
                for(byte[] identifier : previousVerifiers){
                    dependantPreviousVerifiersTable.addRow(
                        "previousVerifier",
                        ByteUtil.arrayAsStringWithDashes(identifier),
                        NyzoStringEncoder.encode(new NyzoStringPublicIdentifier(identifier))
                    );
                }

                
                Map<ByteBuffer, Transaction> pendingCycleTransactions = balanceList.getPendingCycleTransactions();
                for(Transaction transaction : pendingCycleTransactions.values()){
                    List<Object> row = new ArrayList<>();
                    row.add("pendingCycleTransaction");

                    // Add the sender identifier columns.
                    byte[] senderIdentifier = transaction.getSenderIdentifier();
                    row.add(ByteUtil.arrayAsStringWithDashes(senderIdentifier));
                    row.add(NyzoStringEncoder.encode(new NyzoStringPublicIdentifier(senderIdentifier)));

                    // Add the receiver identifier columns.
                    byte[] receiverIdentifier = transaction.getReceiverIdentifier();
                    row.add(ByteUtil.arrayAsStringWithDashes(receiverIdentifier));
                    row.add(NyzoStringEncoder.encode(new NyzoStringPublicIdentifier(receiverIdentifier)));

                    // Add the amount, height, initiator data, number of cycle signatures, and number of "yes" votes.
                    row.add(PrintUtil.printAmount(transaction.getAmount()));
                    row.add(BlockManager.heightForTimestamp(transaction.getTimestamp()));

                    row.add(WebUtil.sanitizedSenderDataForDisplay(transaction.getSenderData()));
                    row.add(transaction.getCycleSignatureTransactions().size());
                    row.add(numberOfYesVotes(transaction.getCycleSignatureTransactions().values()));

                    // Add the signature columns.
                    row.add(ByteUtil.arrayAsStringWithDashes(transaction.getSignature()));
                    row.add(NyzoStringEncoder.encode(new NyzoStringSignature(transaction.getSignature())));

                    // Add the row to the table.
                    dependantPendingCycleTransactionsTable.addRow(row.toArray());
                }

                List<ApprovedCycleTransaction> recentlyApprovedCycleTransactions = balanceList.getRecentlyApprovedCycleTransactions();
                for(ApprovedCycleTransaction transactionStub : recentlyApprovedCycleTransactions){
                    dependantRecentlyApprovedCycleTransactionsTable.addRow(
                        "recentlyApprovedCycleTransactionStub",
                        ByteUtil.arrayAsStringWithDashes(transactionStub.getInitiatorIdentifier()),
                        NyzoStringEncoder.encode(new NyzoStringPublicIdentifier(transactionStub.getInitiatorIdentifier())),
                        ByteUtil.arrayAsStringWithDashes(transactionStub.getReceiverIdentifier()),
                        NyzoStringEncoder.encode(new NyzoStringPublicIdentifier(transactionStub.getReceiverIdentifier())),
                        transactionStub.getApprovalHeight(),
                        PrintUtil.printAmountWithCommas(transactionStub.getAmount())
                    );
                }

            }
        } else {
            errors.add("Could not determine start timestamp for block height " + blockHeight);
        }

        return new SimpleExecutionResult(notices, errors, parentTable, dependantBalanceTable, dependantPreviousVerifiersTable, dependantPendingCycleTransactionsTable, dependantRecentlyApprovedCycleTransactionsTable);
    }

    private static int numberOfYesVotes(Collection<Transaction> signatureTransactions) {
        int numberOfYesVotes = 0;
        for (Transaction transaction : signatureTransactions) {
            if (transaction.getCycleTransactionVote() == 1) {
                numberOfYesVotes++;
            }
        }

        return numberOfYesVotes;
    }
}
