import java.io.*;
import java.util.*;

public class TD3 extends TD2 {
    private boolean inTransaction = false;
    private long recordCountBeforeTransaction;

    public TD3(String filename) throws IOException {
        super(filename);
    }

    public void begin() throws IOException {
        if (inTransaction) commit();
        inTransaction = true;
        recordCountBeforeTransaction = recordCount;
        System.out.println("BEGIN");
    }

    public void commit() throws IOException {
        if (!inTransaction) return;
        for (Map.Entry<Integer, Page> entry : buffer.getAllPages().entrySet()) {
            Page page = entry.getValue();
            if (page.dirty && page.transactional) {
                buffer.force(entry.getKey());
                page.transactional = false;
            }
        }   
        inTransaction = false;
        System.out.println("COMMIT");
    }

    public void rollback() {
        if (!inTransaction) return;
        List<Integer> toEvict = new ArrayList<>();
        for (Map.Entry<Integer, Page> entry : buffer.getAllPages().entrySet()) {
            if (entry.getValue().transactional) {
                toEvict.add(entry.getKey());
            }
        }
        for (int pageId : toEvict) {
            buffer.evict(pageId);
        }
        recordCount = recordCountBeforeTransaction;
        inTransaction = false;
        System.out.println("ROLLBACK");
    }

    @Override
    public void insertRecord(String data) throws IOException {
        super.insertRecord(data);
        if (inTransaction) {
            int pageId = (int) ((recordCount - 1) / RECORDS_PER_PAGE);
            Page page = buffer.fix(pageId);
            page.transactional = true;
            buffer.unfix(pageId);
        }
    }

    public static void main(String[] args) throws IOException {
        TD3 db = new TD3("td3_data.db");

        System.out.println("=== Test COMMIT ===");
        db.begin();
        db.insertRecord("Record transactionnel 1");
        db.insertRecord("Record transactionnel 2");
        db.commit();

        System.out.println("\n=== Test ROLLBACK ===");
        db.begin();
        db.insertRecord("Record qui sera annulé");
        db.rollback();

        System.out.println("\n=== Lecture ===");
        System.out.println("Nombre de records: " + db.getRecordCount());
        for (int i = 0; i < db.getRecordCount(); i++) {
            System.out.println("Record " + i + ": " + db.readRecord(i));
        }

        db.close();
    }
}
