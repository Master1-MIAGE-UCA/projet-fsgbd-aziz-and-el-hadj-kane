import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

class Page {
    byte[] data;
    boolean dirty;
    int fixCount;

    public Page(int pageSize) {
        this.data = new byte[pageSize];
        this.dirty = false;
        this.fixCount = 0;
    }
}

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

    // FIX : ramène la page en mémoire si nécessaire et incrémente fixCount
    public Page fix(int pageId) throws IOException {
        Page page = buffer.get(pageId);
        if (page == null) {
            page = new Page(PAGE_SIZE);
            long pos = HEADER_SIZE + (long) pageId * PAGE_SIZE;
            long fileLen = file.length();

            if (pos < fileLen) {
                file.seek(pos);
                int toRead = (int) Math.min(PAGE_SIZE, fileLen - pos);
                int read = 0;
                while (read < toRead) {
                    int n = file.read(page.data, read, toRead - read);
                    if (n <= 0) break;
                    read += n;
                }
                // si partie incomplète on laisse le reste à 0
            }
            buffer.put(pageId, page);
        }
        page.fixCount++;
        return page;
    }

    // UNFIX : signale qu'on a fini d'utiliser la page
    public void unfix(int pageId) {
        Page page = buffer.get(pageId);
        if (page != null && page.fixCount > 0) {
            page.fixCount--;
        }
    }

    // USE : marque dirty
    public void use(int pageId) {
        Page page = buffer.get(pageId);
        if (page != null) page.dirty = true;
    }

    // FORCE : écriture immédiate si dirty
    public void force(int pageId) throws IOException {
        Page page = buffer.get(pageId);
        if (page != null && page.dirty) {
            long pos = HEADER_SIZE + (long) pageId * PAGE_SIZE;
            file.seek(pos);
            file.write(page.data); // écrit la page complète
            file.getFD().sync(); // s'assure que c'est sur le disque (optionnel mais sûr)
            page.dirty = false;
        }
    }
}

public class MiniSGBD2Fixed implements AutoCloseable {
    private static final int PAGE_SIZE = 4096;
    private static final int RECORD_SIZE = 100;
    private static final int RECORDS_PER_PAGE = PAGE_SIZE / RECORD_SIZE;
    private static final long HEADER_SIZE = 8; // on stocke un long (recordCount)

    private final RandomAccessFile file;
    private final BufferManager buffer;
    private long recordCount; // nombre d'enregistrements déjà insérés (persisté dans le header)

    public MiniSGBD2Fixed(String filename) throws IOException {
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

    public void close() throws IOException {
        // avant de fermer, on peut forcer toutes les pages dirty (non implémenté: parcours buffer)
        persistRecordCount();
        file.close();
    }

    // petit main de test
    public static void main(String[] args) throws IOException {
        // attention: si le fichier existe déjà et contient des données, il sera réutilisé
        try (MiniSGBD2Fixed db = new MiniSGBD2Fixed("etudiants_fixed.db")) {
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
