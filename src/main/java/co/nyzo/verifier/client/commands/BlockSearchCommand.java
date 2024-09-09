package co.nyzo.verifier.client.commands;

import java.util.ArrayList;
import java.util.List;

import co.nyzo.verifier.Block;
import co.nyzo.verifier.BlockFileConsolidator;
import co.nyzo.verifier.BlockManager;
import co.nyzo.verifier.ByteUtil;
import co.nyzo.verifier.CycleDigest;
import co.nyzo.verifier.CycleInformation;
import co.nyzo.verifier.HistoricalBlockManager;
import co.nyzo.verifier.Transaction;
import co.nyzo.verifier.client.CommandOutput;
import co.nyzo.verifier.client.CommandTable;
import co.nyzo.verifier.client.CommandTableHeader;
import co.nyzo.verifier.client.ExecutionResult;
import co.nyzo.verifier.client.SimpleExecutionResult;
import co.nyzo.verifier.client.ValidationResult;

public class BlockSearchCommand implements Command {
    
    @Override
    public String getShortCommand(){
        return "BS";
    }

    @Override
    public String getLongCommand(){
        return "blockSearch";
    }

    @Override
    public String getDescription(){
        return "search for a block";
    }

    @Override
    public String[] getArgumentNames(){
        return new String[] { "block height" };
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

        try {
            blockHeight = Long.parseLong(args.get(0));
        } catch (Exception ignored) {}

        long minimumTimestamp = -1L;
        long maximumTimestamp = -1L;

        if(blockHeight >= 0){
            notices.add("Using block height of " + blockHeight + " for search");
            minimumTimestamp = BlockManager.startTimestampForHeight(blockHeight);
            maximumTimestamp = BlockManager.startTimestampForHeight(blockHeight + 1L) - 1L;
        }

        CommandTable table = new CommandTable(
            new CommandTableHeader("height", "height"),
            new CommandTableHeader("blockchain version", "blockchainVersion"),
            new CommandTableHeader("hash", "hash"),
            new CommandTableHeader("cycle length", "cycleLength"),
            new CommandTableHeader("previous block hash", "previousBlockHash"),
            new CommandTableHeader("start timestamp", "startTimestamp"),
            new CommandTableHeader("end timestamp", "endTimestamp"),
            new CommandTableHeader("verification timestamp", "verificationTimestamp"),
            new CommandTableHeader("balance list hash", "balanceListHash"),
            new CommandTableHeader("verification identifier", "verificationIdentifier"),
            new CommandTableHeader("verification signature", "verificationSignature")
        );

        if(minimumTimestamp > 0){
            Block block = null;

            try {
                block = BlockManager.frozenBlockForHeight(blockHeight);
                
                if(block == null){
                    block = BlockManager.loadBlockFromFile(blockHeight);
                }
                if(block == null){
                    block = HistoricalBlockManager.blockForHeight(blockHeight);
                }
            } catch (Exception e){
                errors.add("Failed to find block");
            }

            long frozenEdgeHeight = BlockManager.getFrozenEdgeHeight();
            long retentionEdgeHeight = BlockManager.getRetentionEdgeHeight();

            if (block == null && blockHeight > frozenEdgeHeight) {
                errors.add("Block " + blockHeight + " is past the frozen edge, " + frozenEdgeHeight + ", on this system");
            } else if (block == null && blockHeight < BlockManager.getRetentionEdgeHeight()) {
                errors.add("Block " + blockHeight + " is behind the retention edge, " + retentionEdgeHeight + ", on this system");
            } else if (block == null) {
                errors.add("Block " + blockHeight + " is not available on this system");
            } else {
                notices.add("Refer to the transactionSearch endpoint to fetch the transactions for this block");
                notices.add("Refer to the balanceList endpoint to fetch the balance list for this block");

                CycleInformation info = block.getCycleInformation(true);

                table.addRow(
                    block.getBlockHeight(),
                    block.getBlockchainVersion(),
                    ByteUtil.arrayAsStringWithDashes(block.getHash()),
                    info.getCycleLength(),
                    ByteUtil.arrayAsStringWithDashes(block.getPreviousBlockHash()),
                    block.getStartTimestamp(),
                    block.getStartTimestamp() + Block.blockDuration,
                    block.getVerificationTimestamp(),
                    ByteUtil.arrayAsStringWithDashes(block.getBalanceListHash()),
                    ByteUtil.arrayAsStringWithDashes(block.getVerifierIdentifier()),
                    ByteUtil.arrayAsStringWithDashes(block.getVerifierSignature())
                );
            }
        } else {
            errors.add("Unable to process query");
        }

        return new SimpleExecutionResult(notices, errors, table);
    }
}
