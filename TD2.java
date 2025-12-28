import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class TD2 implements AutoCloseable {
    protected static final int PAGE_SIZE = 4096;
    protected static final int RECORD_SIZE = 100;
    protected static final int RECORDS_PER_PAGE = PAGE_SIZE / RECORD_SIZE;
    protected static final long HEADER_SIZE = 8;

    protected final RandomAccessFile file;
    protected final BufferManager buffer;
    protected long recordCount;

    public TD2(String filename) throws IOException {
        File f = new File(filename);
        // on ne supprime plus systématiquement le fichier, on ouvre/reprend si existant
        this.file = new RandomAccessFile(filename, "rw");
        if (file.length() < HEADER_SIZE) {
            // init header
            file.setLength(HEADER_SIZE);
            file.seek(0);
            file.writeLong(0L);
            this.recordCount = 0L;
        } else {
            file.seek(0);
            this.recordCount = file.readLong();
        }
        this.buffer = new BufferManager(file, PAGE_SIZE, HEADER_SIZE);
    }

    // écrire le header (persistant)
    private void persistRecordCount() throws IOException {
        file.seek(0);
        file.writeLong(recordCount);
        file.getFD().sync();
    }

    // Insertion "buffer only" (on écrit en mémoire et on met dirty)
    public void insertRecord(String data) throws IOException {
        long totalRecords = recordCount;
        int pageId = (int) (totalRecords / RECORDS_PER_PAGE);
        int slot = (int) (totalRecords % RECORDS_PER_PAGE);

        Page page = buffer.fix(pageId);
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        byte[] record = new byte[RECORD_SIZE];
        System.arraycopy(bytes, 0, record, 0, Math.min(bytes.length, RECORD_SIZE));

        System.arraycopy(record, 0, page.data, slot * RECORD_SIZE, RECORD_SIZE);
        buffer.use(pageId); // marque la page dirty
        buffer.unfix(pageId);

        recordCount++;          // on réserve la place
        persistRecordCount();   // on persiste le compteur (facile et fiable)
    }

    // Insertion synchrone : buffer + écriture immédiate sur disque
    public void insertRecordSync(String data) throws IOException {
        long totalRecords = recordCount;
        int pageId = (int) (totalRecords / RECORDS_PER_PAGE);
        int slot = (int) (totalRecords % RECORDS_PER_PAGE);

        Page page = buffer.fix(pageId);
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        byte[] record = new byte[RECORD_SIZE];
        System.arraycopy(bytes, 0, record, 0, Math.min(bytes.length, RECORD_SIZE));

        System.arraycopy(record, 0, page.data, slot * RECORD_SIZE, RECORD_SIZE);
        buffer.use(pageId);
        buffer.force(pageId); // écriture immédiate de la page
        buffer.unfix(pageId);

        recordCount++;
        persistRecordCount();
    }

    public String readRecord(int id) throws IOException {
        if (id < 0 || id >= recordCount) throw new IOException("Record " + id + " n'existe pas !");
        int pageId = id / RECORDS_PER_PAGE;
        int slot = id % RECORDS_PER_PAGE;

        Page page = buffer.fix(pageId);
        byte[] record = Arrays.copyOfRange(page.data, slot * RECORD_SIZE, (slot + 1) * RECORD_SIZE);
        buffer.unfix(pageId);
        return new String(record, StandardCharsets.UTF_8).trim();
    }

    public List<String> getPage(int pageId) throws IOException {
        List<String> records = new ArrayList<>();
        Page page = buffer.fix(pageId);

        for (int i = 0; i < RECORDS_PER_PAGE; i++) {
            byte[] record = Arrays.copyOfRange(page.data, i * RECORD_SIZE, (i + 1) * RECORD_SIZE);
            String s = new String(record, StandardCharsets.UTF_8).trim();
            if (!s.isEmpty()) records.add(s);
        }
        buffer.unfix(pageId);
        return records;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public void close() throws IOException {
        // avant de fermer, on peut forcer toutes les pages dirty (non implémenté: parcours buffer)
        persistRecordCount();
        file.close();
    }

    // petit main de test
    public static void main(String[] args) throws IOException {
        // attention: si le fichier existe déjà et contient des données, il sera réutilisé
        try (TD2 db = new TD2("etudiants_fixed.db")) {
            System.out.println("Insertion synchrone de 5 étudiants...");
            for (int i = 1; i <= 5; i++) {
                db.insertRecordSync("Etudiant " + i);
            }

            System.out.println("Lecture de l'étudiant 3 : " + db.readRecord(2));
            List<String> page0 = db.getPage(0);
            System.out.println("Contenu de la page 0 : " + page0);

            // pour vérifier où sont les autres (au cas où) :
            List<String> page1 = db.getPage(1);
            System.out.println("Contenu de la page 1 : " + page1);
        }
    }
}
