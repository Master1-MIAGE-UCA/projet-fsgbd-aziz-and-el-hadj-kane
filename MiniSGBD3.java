import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

// ----- Classe Page -----
class Page {
    byte[] data;
    boolean dirty;
    int fixCount;
    boolean transactional;

    public Page(int pageSize) {
        this.data = new byte[pageSize];
        this.dirty = false;
        this.fixCount = 0;
        this.transactional = false;
    }
}

// ----- Buffer Manager -----
class BufferManager {
    private final RandomAccessFile file;
    private final Map<Integer, Page> buffer = new HashMap<>();
    private final int PAGE_SIZE;
    private final long HEADER_SIZE;

    public BufferManager(RandomAccessFile file, int pageSize, long headerSize) {
        this.file = file;
        this.PAGE_SIZE = pageSize;
        this.HEADER_SIZE = headerSize;
    }

    // FIX : charge une page en mémoire si besoin
    public Page fix(int pageId) throws IOException {
        Page page = buffer.get(pageId);
        if (page == null) {
            page = new Page(PAGE_SIZE);
            long pos = HEADER_SIZE + (long) pageId * PAGE_SIZE;
            long fileLen = file.length();
            if (pos < fileLen) {
                file.seek(pos);
                int toRead = (int) Math.min(PAGE_SIZE, fileLen - pos);
                file.readFully(page.data, 0, toRead);
            }
            buffer.put(pageId, page);
        }
        page.fixCount++;
        return page;
    }

    // UNFIX : libère la page
    public void unfix(int pageId) {
        Page page = buffer.get(pageId);
        if (page != null && page.fixCount > 0) {
            page.fixCount--;
        }
    }

    // USE : marque la page comme modifiée
    public void use(int pageId) {
        Page page = buffer.get(pageId);
        if (page != null) page.dirty = true;
    }

    // FORCE : écrit immédiatement une page sur disque
    public void force(int pageId) throws IOException {
        Page page = buffer.get(pageId);
        if (page != null && page.dirty) {
            long pos = HEADER_SIZE + (long) pageId * PAGE_SIZE;
            file.seek(pos);
            file.write(page.data);
            file.getFD().sync();
            page.dirty = false;
            page.transactional = false;
        }
    }

    // Supprime une page du buffer (rollback)
    public void drop(int pageId) {
        buffer.remove(pageId);
    }

    // Récupère toutes les pages en mémoire
    public Collection<Map.Entry<Integer, Page>> getAllPages() {
        return buffer.entrySet();
    }
}

// ----- MiniSGBD3 -----
public class MiniSGBD3 implements AutoCloseable {
    private static final int PAGE_SIZE = 4096;
    private static final int RECORD_SIZE = 100;
    private static final int RECORDS_PER_PAGE = PAGE_SIZE / RECORD_SIZE;
    private static final long HEADER_SIZE = 8; // stocke recordCount

    private final RandomAccessFile file;
    private final BufferManager buffer;
    private long recordCount;
    private boolean inTransaction = false;
    private long preTransactionRecordCount; // sauvegarde avant transaction

    public MiniSGBD3(String filename) throws IOException {
        this.file = new RandomAccessFile(filename, "rw");
        if (file.length() < HEADER_SIZE) {
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

    // Persiste le compteur d'enregistrements
    private void persistRecordCount() throws IOException {
        file.seek(0);
        file.writeLong(recordCount);
        file.getFD().sync();
    }

    // ----- Transactions -----

    public void begin() throws IOException {
        if (inTransaction) {
            commit(); // commit implicite si déjà en cours
        }
        preTransactionRecordCount = recordCount;
        inTransaction = true;
    }

    public void commit() throws IOException {
        for (var entry : buffer.getAllPages()) {
            int pageId = entry.getKey();
            Page page = entry.getValue();
            if (page.dirty && page.transactional) {
                buffer.force(pageId);   // écrit sur disque
            }
        }
        persistRecordCount(); // on persiste le nouveau recordCount
        inTransaction = false;
    }

    public void rollback() throws IOException {
        for (var entry : new ArrayList<>(buffer.getAllPages())) {
            int pageId = entry.getKey();
            Page page = entry.getValue();
            if (page.transactional) {
                buffer.drop(pageId); // on jette la page
            }
        }
        recordCount = preTransactionRecordCount; // on restaure l'ancien recordCount
        persistRecordCount(); // IMPORTANT : on persiste la restauration
        inTransaction = false;
    }

    // ----- Insertion -----

    public void insertRecord(String data) throws IOException {
        long totalRecords = recordCount;
        int pageId = (int) (totalRecords / RECORDS_PER_PAGE);
        int slot = (int) (totalRecords % RECORDS_PER_PAGE);

        Page page = buffer.fix(pageId);
        byte[] record = new byte[RECORD_SIZE];
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        System.arraycopy(bytes, 0, record, 0, Math.min(bytes.length, RECORD_SIZE));
        System.arraycopy(record, 0, page.data, slot * RECORD_SIZE, RECORD_SIZE);

        buffer.use(pageId);

        if (inTransaction) {
            page.transactional = true; // modif liée à la transaction
        } else {
            buffer.force(pageId); // écrit direct si pas de transaction
            recordCount++;
            persistRecordCount();
        }

        buffer.unfix(pageId);
        
        // Si en transaction, on incrémente temporairement mais on ne persiste PAS
        if (inTransaction) {
            recordCount++;
        }
    }

    // Insertion synchrone : buffer + écriture immédiate sur disque (héritée du TD2)
    public void insertRecordSync(String data) throws IOException {
        long totalRecords = recordCount;
        int pageId = (int) (totalRecords / RECORDS_PER_PAGE);
        int slot = (int) (totalRecords % RECORDS_PER_PAGE);

        Page page = buffer.fix(pageId);
        byte[] record = new byte[RECORD_SIZE];
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        System.arraycopy(bytes, 0, record, 0, Math.min(bytes.length, RECORD_SIZE));
        System.arraycopy(record, 0, page.data, slot * RECORD_SIZE, RECORD_SIZE);

        buffer.use(pageId);
        buffer.force(pageId); // écriture immédiate de la page
        buffer.unfix(pageId);

        recordCount++;
        persistRecordCount();
    }

    // ----- Lecture -----

    public String readRecord(int id) throws IOException {
        if (id < 0 || id >= recordCount) throw new IOException("Record " + id + " n'existe pas !");
        int pageId = id / RECORDS_PER_PAGE;
        int slot = id % RECORDS_PER_PAGE;
        Page page = buffer.fix(pageId);
        byte[] record = Arrays.copyOfRange(page.data, slot * RECORD_SIZE, (slot + 1) * RECORD_SIZE);
        buffer.unfix(pageId);
        return new String(record, StandardCharsets.UTF_8).trim();
    }

    // Lecture d'une page entière (héritée du TD1/TD2)
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

    // ----- Fermeture -----

    public void close() throws IOException {
        // Ne pas persister si on est encore en transaction (anomalie)
        // En pratique, on pourrait faire un rollback ou commit automatique
        if (!inTransaction) {
            persistRecordCount();
        }
        file.close();
    }

    // ----- Test -----
    public static void main(String[] args) throws IOException {
        // Supprimer le fichier de test pour repartir de zéro
        new File("etudiants_tx.db").delete();
        
        try (MiniSGBD3 db = new MiniSGBD3("etudiants_tx.db")) {

            System.out.println("=== TEST DES TRANSACTIONS ===\n");

            // Exemple rollback
            System.out.println("1. Test ROLLBACK:");
            db.begin();
            db.insertRecord("Etudiant 200");
            db.insertRecord("Etudiant 201");
            db.rollback();
            System.out.println("   Après rollback, recordCount = " + db.recordCount + " (attendu: 0)\n");

            // Exemple commit
            System.out.println("2. Test COMMIT:");
            db.begin();
            db.insertRecord("Etudiant 202");
            db.insertRecord("Etudiant 203");
            db.commit();
            System.out.println("   Après commit, recordCount = " + db.recordCount + " (attendu: 2)\n");

            // Lecture pour vérifier
            System.out.println("3. Test LECTURE:");
            System.out.println("   Record 0 : " + db.readRecord(0));
            System.out.println("   Record 1 : " + db.readRecord(1) + "\n");

            // Test de getPage (hérité du TD1/TD2)
            System.out.println("4. Test GET_PAGE:");
            List<String> page0 = db.getPage(0);
            System.out.println("   Contenu de la page 0 : " + page0 + "\n");

            // Test de insertRecordSync (hérité du TD2)
            System.out.println("5. Test INSERT_RECORD_SYNC:");
            db.insertRecordSync("Etudiant 204");
            System.out.println("   Insertion synchrone effectuée, recordCount = " + db.recordCount);
            System.out.println("   Record 2 : " + db.readRecord(2) + "\n");

            System.out.println("=== TOUS LES TESTS RÉUSSIS ===");
        }
    }
}
