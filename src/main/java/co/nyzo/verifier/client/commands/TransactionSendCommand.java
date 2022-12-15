package co.nyzo.verifier.client.commands;

import co.nyzo.verifier.*;
import co.nyzo.verifier.client.*;
import co.nyzo.verifier.nyzoString.NyzoString;
import co.nyzo.verifier.nyzoString.NyzoStringEncoder;
import co.nyzo.verifier.nyzoString.NyzoStringPrivateSeed;
import co.nyzo.verifier.nyzoString.NyzoStringPublicIdentifier;
import co.nyzo.verifier.util.PrintUtil;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class TransactionSendCommand implements Command {

    @Override
    public String getShortCommand() {
        return "ST";
    }

    @Override
    public String getLongCommand() {
        return "send";
    }

    @Override
    public String getDescription() {
        return "send a standard transaction";
    }

    @Override
    public String[] getArgumentNames() {
        return new String[] { "sender key", "receiver ID", "sender data", "amount, Nyzos" };
    }

    @Override
    public String[] getArgumentIdentifiers() {
        return new String[] { "senderKey", "receiverId", "senderData", "amountNyzos" };
    }

    @Override
    public boolean requiresValidation() {
        return true;
    }

    @Override
    public boolean requiresConfirmation() {
        return true;
    }

    @Override
    public boolean isLongRunning() {
        return true;
    }

    @Override
    public ValidationResult validate(List<String> argumentValues, CommandOutput output) {

        ValidationResult result = null;
        try {
            // Make a list for the argument result items.
            List<ArgumentResult> argumentResults = new ArrayList<>();

            // Check the sender key.
            NyzoString senderKey = NyzoStringEncoder.decode(argumentValues.get(0));
            if (senderKey instanceof NyzoStringPrivateSeed) {
                argumentResults.add(new ArgumentResult(true, NyzoStringEncoder.encode(senderKey)));
            } else {
                String message = argumentValues.get(0).trim().isEmpty() ? "missing Nyzo string private key" :
                        "not a valid Nyzo string private key";
                argumentResults.add(new ArgumentResult(false, argumentValues.get(0), message));

                if (argumentValues.get(0).length() >= 64) {
                    PrivateNyzoStringCommand.printHexWarning(output);
                }
            }

            // Check the receiver ID.
            NyzoString receiverIdentifier = NyzoStringEncoder.decode(argumentValues.get(1));
            if (receiverIdentifier instanceof NyzoStringPublicIdentifier) {
                boolean receiverIsValid = true;
                String receiverError = "";
                if (senderKey instanceof NyzoStringPrivateSeed) {
                    byte[] senderId = KeyUtil.identifierForSeed(((NyzoStringPrivateSeed) senderKey).getSeed());
                    if (ByteUtil.arraysAreEqual(senderId,
                            ((NyzoStringPublicIdentifier) receiverIdentifier).getIdentifier())) {
                        receiverIsValid = false;
                        receiverError = "sender and receiver are same";
                    }
                }

                argumentResults.add(new ArgumentResult(receiverIsValid, NyzoStringEncoder.encode(receiverIdentifier),
                        receiverError));
            } else {
                String message = argumentValues.get(1).trim().isEmpty() ? "missing Nyzo string public ID" :
                        "not a valid Nyzo string public ID";
                argumentResults.add(new ArgumentResult(false, argumentValues.get(1), message));

                if (argumentValues.get(1).length() >= 64) {
                    PublicNyzoStringCommand.printHexWarning(output);
                }
            }

            // Process the sender data.
            String senderData = argumentValues.get(2).trim();
            String senderDataMessage = "";
            if (ClientTransactionUtil.isNormalizedSenderDataString(senderData)) {
                senderData = "X" + senderData.toLowerCase().substring(1);
            } else {
                byte[] senderDataBytes = argumentValues.get(2).getBytes(StandardCharsets.UTF_8);
                if (senderDataBytes.length > FieldByteSize.maximumSenderDataLength) {
                    senderDataMessage = "sender data too long; truncating";
                    senderDataBytes = Arrays.copyOf(senderDataBytes, FieldByteSize.maximumSenderDataLength);
                    senderData = new String(senderDataBytes, StandardCharsets.UTF_8);
                }
            }
            argumentResults.add(new ArgumentResult(true, senderData, senderDataMessage));

            // Check the amount.
            long amountMicronyzos = -1L;
            try {
                amountMicronyzos = (long) (Double.parseDouble(argumentValues.get(3)) *
                        Transaction.micronyzoMultiplierRatio);
            } catch (Exception ignored) { }
            if (amountMicronyzos > 0) {
                double amountNyzos = amountMicronyzos / (double) Transaction.micronyzoMultiplierRatio;
                argumentResults.add(new ArgumentResult(true, String.format("%.6f", amountNyzos), ""));
            } else {
                argumentResults.add(new ArgumentResult(false, argumentValues.get(3), "invalid amount"));
            }

            // Produce the result.
            result = new ValidationResult(argumentResults);

        } catch (Exception ignored) { }

        // If the confirmation result is null, create an exception result. This will only happen if an exception is not
        // handled properly by the validation code.
        if (result == null) {
            result = ValidationResult.exceptionResult(getArgumentNames().length);
        }

        return result;
    }

    @Override
    public ExecutionResult run(List<String> argumentValues, CommandOutput output) {

        try {
            // Get the arguments.
            NyzoStringPrivateSeed signerSeed = (NyzoStringPrivateSeed) NyzoStringEncoder.decode(argumentValues.get(0));
            NyzoStringPublicIdentifier receiverIdentifier =
                    (NyzoStringPublicIdentifier) NyzoStringEncoder.decode(argumentValues.get(1));
            byte[] senderData = ClientArgumentUtil.getSenderData(argumentValues.get(2));
            long amount = (long) (Double.parseDouble(argumentValues.get(3)) * Transaction.micronyzoMultiplierRatio);

            // Send the transaction to the cycle.
            ClientTransactionUtil.createAndSendTransaction(signerSeed, receiverIdentifier, senderData, amount, output);
        } catch (Exception e) {
            output.println(ConsoleColor.Red + "unexpected issue creating transaction: " + PrintUtil.printException(e) +
                    ConsoleColor.reset);
        }

        // ExecutionResult objects are not yet implemented for long-running commands.
        return null;
    }
}
