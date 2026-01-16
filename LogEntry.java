import java.io.*;
import java.util.Arrays;

public class LogEntry {
    // Types d'entrées du journal
    public enum LogType {
        BEGIN, COMMIT, ROLLBACK, UPDATE, INSERT, CHECKPOINT
    }

    // Champs de l'entrée du journal
    public long lsn;              // Log Sequence Number
    public LogType type;          // Type d'opération
    public int transactionId;     // ID de la transaction
    public int recordId;          // ID de l'enregistrement (optionnel)
    public byte[] beforeImage;    // Image avant modification (optionnel)
    public byte[] afterImage;     // Image après modification (optionnel)

    // Constructeur pour BEGIN, COMMIT, ROLLBACK
    public LogEntry(int transactionId, LogType type, long lsn) {
        this.transactionId = transactionId;
        this.type = type;
        this.lsn = lsn;
        this.recordId = -1;  // Non applicable
        this.beforeImage = null;
        this.afterImage = null;
    }

    // Constructeur pour CHECKPOINT
    public LogEntry(LogType type, long lsn) {
        this.transactionId = -1;  // Non applicable
        this.type = type;
        this.lsn = lsn;
        this.recordId = -1;
        this.beforeImage = null;
        this.afterImage = null;
    }

    // Constructeur pour UPDATE et INSERT
    public LogEntry(int transactionId, int recordId, byte[] beforeImage, byte[] afterImage, LogType type, long lsn) {
        this.transactionId = transactionId;
        this.recordId = recordId;
        this.beforeImage = beforeImage;
        this.afterImage = afterImage;
        this.type = type;
        this.lsn = lsn;
    }

    // Sérialisation en bytes pour écriture sur disque
    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        dos.writeLong(lsn);
        dos.writeInt(type.ordinal());
        dos.writeInt(transactionId);
        dos.writeInt(recordId);

        // Écrire beforeImage
        if (beforeImage != null) {
            dos.writeInt(beforeImage.length);
            dos.write(beforeImage);
        } else {
            dos.writeInt(0);
        }

        // Écrire afterImage
        if (afterImage != null) {
            dos.writeInt(afterImage.length);
            dos.write(afterImage);
        } else {
            dos.writeInt(0);
        }

        return baos.toByteArray();
    }

    // Désérialisation depuis bytes
    public static LogEntry fromBytes(byte[] data) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        long lsn = dis.readLong();
        LogType type = LogType.values()[dis.readInt()];
        int transactionId = dis.readInt();
        int recordId = dis.readInt();

        // Lire beforeImage
        int beforeLen = dis.readInt();
        byte[] beforeImage = null;
        if (beforeLen > 0) {
            beforeImage = new byte[beforeLen];
            dis.readFully(beforeImage);
        }

        // Lire afterImage
        int afterLen = dis.readInt();
        byte[] afterImage = null;
        if (afterLen > 0) {
            afterImage = new byte[afterLen];
            dis.readFully(afterImage);
        }

        return new LogEntry(transactionId, recordId, beforeImage, afterImage, type, lsn);
    }

    @Override
    public String toString() {
        return String.format("LogEntry{lsn=%d, type=%s, tx=%d, record=%d}",
                lsn, type, transactionId, recordId);
    }
}