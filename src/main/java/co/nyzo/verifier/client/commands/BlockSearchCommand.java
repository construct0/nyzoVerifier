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

        if(blockHeight >= 0){
            notices.add("Using block height of " + blockHeight + " for search");
            minimumTimestamp = BlockManager.startTimestampForHeight(blockHeight);
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
                // Attempt to get the frozen block from memory
                block = BlockManager.frozenBlockForHeight(blockHeight);

                // Attempt to get the frozen block without writing individual files to disk in the process
                if(block == null){
                    block = HistoricalBlockManager.blockForHeight(blockHeight);
                }

                // Attempt to get the frozen block by extracting the consolidated file for that height, which writes all individual blocks contained therein to disk in the process
                // If the block file consolidator consolidated individual block files these will be consolidated again in the future
                if(block == null){
                    block = BlockManager.loadBlockFromFile(blockHeight);
                }

                // Failed to find block, adding an error to indicate no exception occurred
                if(block == null){
                    errors.add("Failed to find block for height " + blockHeight);
                }
            } catch (Exception e){
                block = null;
            }

            long frozenEdgeHeight = BlockManager.getFrozenEdgeHeight();

            if (block == null && blockHeight > frozenEdgeHeight) {
                errors.add("Block " + blockHeight + " is past the frozen edge, " + frozenEdgeHeight + ", on this system");
            } else if (block == null) {
                errors.add("Block " + blockHeight + " is not available on this system");
            } else {
                notices.add("Refer to the transactionSearch command/endpoint to fetch the transactions for this block");

                // Depending on the cycle length this may require a non-negligible amount of system resources to calculate
                CycleInformation cycleInformation = block.getCycleInformation(true);
                int cycleLength = -1;

                if(cycleInformation == null){
                    notices.add("Cycle length for this block is not available due to an insufficient amount of historical blocks prior to block height " + blockHeight + " being present on this system");
                } else {
                    cycleLength = cycleInformation.getCycleLength();
                }

                table.addRow(
                    block.getBlockHeight(),
                    block.getBlockchainVersion(),
                    ByteUtil.arrayAsStringWithDashes(block.getHash()),
                    cycleLength,
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
