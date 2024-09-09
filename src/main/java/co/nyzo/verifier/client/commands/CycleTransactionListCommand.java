package co.nyzo.verifier.client.commands;

import co.nyzo.verifier.*;
import co.nyzo.verifier.client.*;
import co.nyzo.verifier.nyzoString.*;
import co.nyzo.verifier.util.PrintUtil;
import co.nyzo.verifier.web.WebUtil;

import java.util.*;

public class CycleTransactionListCommand implements Command {

    @Override
    public String getShortCommand() {
        return "CTL";
    }

    @Override
    public String getLongCommand() {
        return "cycleList";
    }

    @Override
    public String getDescription() {
        return "list pending cycle transactions";
    }

    @Override
    public String[] getArgumentNames() {
        return new String[] { "block height (optional)" };
    }

    @Override
    public String[] getArgumentIdentifiers() {
        return new String[] { "blockHeight" };
    }

    @Override
    public boolean requiresValidation() {
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
    public ExecutionResult run(List<String> argumentValues, CommandOutput output) {

        // Make the lists for the notices and errors. Make the result table.
        List<String> notices = new ArrayList<>();
        List<String> errors = new ArrayList<>();

        long blockHeight = -1L;
        Block frozenEdge = BlockManager.getFrozenEdge();
        long frozenEdgeHeight = frozenEdge.getBlockHeight();

        try {
            blockHeight = Long.parseLong(argumentValues.get(0));
        } catch (Exception ignored) {}

        long minimumTimestamp = -1L;
        long maximumTimestamp = -1L;

        if(blockHeight >= 0){
            notices.add("Using block height of " + blockHeight + " for search");
        } else {
            notices.add("Using block height of " + frozenEdgeHeight + " for search");
            blockHeight = frozenEdgeHeight;
        }

        CommandTable table = new CommandTable(
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

        try {
            minimumTimestamp = BlockManager.startTimestampForHeight(blockHeight);
            maximumTimestamp = BlockManager.startTimestampForHeight(blockHeight + 1L) - 1L;

            if(minimumTimestamp <= 0){
                throw new Exception("Could not determine start timestamp for block height " + blockHeight);
            }

            BalanceList balanceList = null;

            if(frozenEdgeHeight == blockHeight){
                balanceList = BalanceListManager.getFrozenEdgeList();
            } else {
                balanceList = BlockManager.loadBalanceListFromFileForHeight(blockHeight);

                if(balanceList != null){
                    notices.add("The pending cycle transaction states in this result are a historical representation, refer to the transactionSearch endpoint to fetch, among others, approved cycle transaction executions; refer to the balanceList endpoint to fetch approved cycle transaction stubs");
                }
            }

            if (balanceList == null) {
                errors.add("balance list is null");
            } else if (balanceList.getPendingCycleTransactions().isEmpty()) {
                notices.add("no cycle transactions in balance list");
            } else {
                long distanceFromOpen = BlockManager.openEdgeHeight(false) - balanceList.getBlockHeight();
                notices.add("using balance list at height " + balanceList.getBlockHeight() + ", " + distanceFromOpen +
                        " from open edge");
                for (Transaction transaction : balanceList.getPendingCycleTransactions().values()) {
                    List<Object> row = new ArrayList<>();

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
                    table.addRow(row.toArray());
                }
            }

        } catch (Exception e) {
            output.println(ConsoleColor.Red + "unexpected issue listing cycle transactions: " +
                    PrintUtil.printException(e) + ConsoleColor.reset);
        }

        return new SimpleExecutionResult(notices, errors, table);
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
