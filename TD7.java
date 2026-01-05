import java.io.*;
import java.util.*;

/**
 * TD7 - Journalisation, Checkpoints et Récupération (UNDO/REDO)
 * 
 * Cette classe étend TD6 pour ajouter :
 * - TJT (Journal de Transactions en mémoire) : liste des LogEntry
 * - FJT (Fichier Journal de Transactions) : persistance du journal sur disque
 * - Checkpoints : points de sauvegarde pour limiter le REDO
 * - Récupération après panne : algorithme UNDO/REDO
 */
public class TD7 extends TD6 {
    
    // TJT : Journal de Transactions en mémoire (Tampon Journal Transactions)
    private List<LogEntry> TJT = new ArrayList<>();
    
    // Fichier journal de transactions (FJT)
    private final String logFilename;
    private RandomAccessFile logFile;
    
    // Numéro de séquence du log (LSN - Log Sequence Number)
    private long currentLSN = 0;
    
    // Position du dernier checkpoint dans le fichier journal
    private long lastCheckpointPosition = 0;
    
    // Compteur d'enregistrements sauvegardé pour la récupération
    private long savedRecordCount = 0;

    public TD7(String filename) throws IOException {
        super(filename);
        this.logFilename = filename + ".log";
        initLogFile();
    }
    
    /**
     * Initialise le fichier journal
     */
    private void initLogFile() throws IOException {
        File f = new File(logFilename);
        boolean exists = f.exists() && f.length() > 0;
        logFile = new RandomAccessFile(logFilename, "rw");
        
        if (exists) {
            // Charger le dernier LSN depuis le fichier
            loadLastLSN();
        }
    }
    
    /**
     * Charge le dernier LSN depuis le fichier journal existant
     */
    private void loadLastLSN() {
        try {
            List<LogEntry> entries = readLogFile();
            if (!entries.isEmpty()) {
                currentLSN = entries.get(entries.size() - 1).lsn;
            }
        } catch (Exception e) {
            currentLSN = 0;
        }
    }
    
    /**
     * Génère un nouveau LSN
     */
    private long nextLSN() {
        return ++currentLSN;
    }
    
    /**
     * Ajoute une entrée au journal en mémoire (TJT)
     */
    private void log(LogEntry entry) {
        TJT.add(entry);
        System.out.println("  [LOG] " + entry);
    }
    
    /**
     * Force l'écriture du TJT dans le FJT (fichier journal)
     */
    private void flushLog() throws IOException {
        if (TJT.isEmpty()) return;
        
        logFile.seek(logFile.length());
        
        for (LogEntry entry : TJT) {
            byte[] data = entry.toBytes();
            // Écrire la taille puis les données
            logFile.writeInt(data.length);
            logFile.write(data);
        }
        
        logFile.getFD().sync();
        System.out.println("  [FJT] Journal forcé sur disque (" + TJT.size() + " entrées)");
        
        TJT.clear();
    }
    
    /**
     * Lit toutes les entrées du fichier journal
     */
    private List<LogEntry> readLogFile() throws IOException, ClassNotFoundException {
        List<LogEntry> entries = new ArrayList<>();
        logFile.seek(0);
        
        while (logFile.getFilePointer() < logFile.length()) {
            try {
                int size = logFile.readInt();
                byte[] data = new byte[size];
                logFile.readFully(data);
                LogEntry entry = LogEntry.fromBytes(data);
                entries.add(entry);
            } catch (EOFException e) {
                break;
            }
        }
        
        return entries;
    }
    
    /**
     * BEGIN - Démarre une nouvelle transaction avec journalisation
     */
    @Override
    public void begin() throws IOException {
        if (inTransaction) commit();
        inTransaction = true;
        currentTransactionId++;
        recordCountBeforeTransaction = recordCount;
        
        // Journaliser le BEGIN
        log(new LogEntry(currentTransactionId, LogEntry.LogType.BEGIN, nextLSN()));
        
        System.out.println("BEGIN (Transaction #" + currentTransactionId + ")");
    }
    
    /**
     * COMMIT - Valide la transaction avec journalisation
     * 
     * IMPORTANT (TD7) : Le COMMIT ne force plus l'écriture des pages sur disque !
     * C'est le journal qui garantit la durabilité.
     */
    @Override
    public void commit() throws IOException {
        if (!inTransaction) return;
        
        System.out.println("COMMIT (Transaction #" + currentTransactionId + ")");
        
        // 1. Journaliser le COMMIT
        log(new LogEntry(currentTransactionId, LogEntry.LogType.COMMIT, nextLSN()));
        
        // 2. Forcer le journal sur disque (Write-Ahead Logging)
        flushLog();
        
        // 3. NE PAS forcer les pages sur disque (différé jusqu'au checkpoint)
        // Les pages restent dirty dans le buffer
        for (Map.Entry<Integer, Page> entry : buffer.getAllPages().entrySet()) {
            Page page = entry.getValue();
            if (page.transactional) {
                page.transactional = false;
                // page.dirty reste true - sera écrit au prochain checkpoint
            }
        }
        
        // 4. Libérer les verrous
        releaseAllLocks();
        
        // 5. Vider le TIV
        clearTIV();
        
        inTransaction = false;
    }
    
    /**
     * ROLLBACK - Annule la transaction avec journalisation
     */
    @Override
    public void rollback() {
        if (!inTransaction) return;
        
        System.out.println("ROLLBACK (Transaction #" + currentTransactionId + ")");
        
        // 1. Restaurer depuis le TIV (comme TD6)
        restoreFromTIV();
        
        // 2. Journaliser le ROLLBACK
        log(new LogEntry(currentTransactionId, LogEntry.LogType.ROLLBACK, nextLSN()));
        
        // 3. Forcer le journal sur disque
        try {
            flushLog();
        } catch (IOException e) {
            System.err.println("Erreur lors du flush du journal: " + e.getMessage());
        }
        
        // 4. Libérer les verrous
        releaseAllLocks();
        
        // 5. Vider le TIV
        clearTIV();
        
        // 6. Restaurer le compteur
        recordCount = recordCountBeforeTransaction;
        
        inTransaction = false;
    }
    
    // Méthodes helper pour accéder aux champs protégés de TD6
    
    protected void releaseAllLocks() {
        locks.clear();
        lockOwners.clear();
    }
    
    protected void clearTIV() {
        TIV.clear();
    }
    
    protected void restoreFromTIV() {
        for (Map.Entry<Integer, byte[]> entry : TIV.entrySet()) {
            int pageId = entry.getKey();
            byte[] imageBefore = entry.getValue();
            
            Page page = buffer.getAllPages().get(pageId);
            if (page != null) {
                System.arraycopy(imageBefore, 0, page.data, 0, imageBefore.length);
                page.dirty = false;
                page.transactional = false;
                System.out.println("  [RESTORE] Page " + pageId + " restaurée depuis TIV");
            }
        }
    }
    
    /**
     * Mise à jour d'un enregistrement avec journalisation
     */
    @Override
    public void updateRecord(int id, String newData) throws IOException {
        if (id < 0 || id >= recordCount) {
            throw new IOException("Record " + id + " n'existe pas !");
        }
        
        int pageId = id / RECORDS_PER_PAGE;
        int slot = id % RECORDS_PER_PAGE;
        int recordId = pageId * RECORDS_PER_PAGE + slot;
        
        // Lire l'image avant
        Page page = buffer.fix(pageId);
        byte[] beforeImage = Arrays.copyOfRange(page.data, slot * RECORD_SIZE, (slot + 1) * RECORD_SIZE);
        buffer.unfix(pageId);
        
        // Effectuer la mise à jour (appelle TD6.updateRecord)
        super.updateRecord(id, newData);
        
        // Lire l'image après
        page = buffer.fix(pageId);
        byte[] afterImage = Arrays.copyOfRange(page.data, slot * RECORD_SIZE, (slot + 1) * RECORD_SIZE);
        buffer.unfix(pageId);
        
        // Journaliser l'UPDATE
        log(new LogEntry(currentTransactionId, recordId, beforeImage, afterImage, 
                         LogEntry.LogType.UPDATE, nextLSN()));
    }
    
    /**
     * Insertion d'un enregistrement avec journalisation
     */
    @Override
    public void insertRecord(String data) throws IOException {
        long recordId = recordCount;
        int pageId = (int) (recordId / RECORDS_PER_PAGE);
        int slot = (int) (recordId % RECORDS_PER_PAGE);
        
        // Effectuer l'insertion
        super.insertRecord(data);
        
        // Lire l'image après
        Page page = buffer.fix(pageId);
        byte[] afterImage = Arrays.copyOfRange(page.data, slot * RECORD_SIZE, (slot + 1) * RECORD_SIZE);
        buffer.unfix(pageId);
        
        // Journaliser l'INSERT (pas d'image avant pour un INSERT)
        if (inTransaction) {
            log(new LogEntry(currentTransactionId, (int) recordId, null, afterImage, 
                             LogEntry.LogType.INSERT, nextLSN()));
        }
    }
    
    /**
     * CHECKPOINT - Point de sauvegarde
     * 
     * 1. Force toutes les pages dirty sur disque
     * 2. Sauvegarde le recordCount
     * 3. Journalise le CHECKPOINT
     * 4. Force le journal sur disque
     */
    public void checkpoint() throws IOException {
        System.out.println("\n=== CHECKPOINT ===");
        
        // 1. Forcer toutes les pages dirty sur disque
        int pagesWritten = 0;
        for (Map.Entry<Integer, Page> entry : buffer.getAllPages().entrySet()) {
            Page page = entry.getValue();
            if (page.dirty) {
                buffer.force(entry.getKey());
                pagesWritten++;
                System.out.println("  [FORCE] Page " + entry.getKey() + " écrite sur disque");
            }
        }
        
        // 2. Persister le recordCount
        persistRecordCount();
        savedRecordCount = recordCount;
        
        // 3. Journaliser le CHECKPOINT
        log(new LogEntry(LogEntry.LogType.CHECKPOINT, nextLSN()));
        
        // 4. Mémoriser la position du checkpoint
        lastCheckpointPosition = logFile.length();
        
        // 5. Forcer le journal
        flushLog();
        
        System.out.println("  [CHECKPOINT] " + pagesWritten + " pages écrites, recordCount=" + recordCount);
        System.out.println("==================\n");
    }
    
    /**
     * Persiste le recordCount dans le header du fichier
     */
    private void persistRecordCount() throws IOException {
        // Accès au fichier via réflexion
        try {
            java.lang.reflect.Field fileField = TD2.class.getDeclaredField("file");
            fileField.setAccessible(true);
            RandomAccessFile dataFile = (RandomAccessFile) fileField.get(this);
            dataFile.seek(0);
            dataFile.writeLong(recordCount);
            dataFile.getFD().sync();
        } catch (Exception e) {
            System.err.println("Erreur persistRecordCount: " + e.getMessage());
        }
    }
    
    /**
     * CRASH - Simule un crash système
     * 
     * Vide tous les buffers sans les écrire sur disque
     */
    public void crash() {
        System.out.println("\n!!! CRASH SYSTÈME !!!");
        
        // Vider le buffer sans écriture
        buffer.getAllPages().clear();
        
        // Vider le TJT (perdu en cas de crash)
        TJT.clear();
        
        // Vider le TIV
        clearTIV();
        
        // Libérer les verrous
        releaseAllLocks();
        
        // Réinitialiser l'état transactionnel
        inTransaction = false;
        
        System.out.println("  Buffers vidés, état volatile perdu");
        System.out.println("  Le fichier journal (FJT) est intact\n");
    }
    
    /**
     * RECOVER - Récupération après panne
     * 
     * 1. Analyse du journal pour identifier les transactions committées et non committées
     * 2. REDO : rejouer les opérations des transactions committées
     * 3. UNDO : annuler les opérations des transactions non committées
     */
    public void recover() throws IOException, ClassNotFoundException {
        System.out.println("\n========================================");
        System.out.println("         RÉCUPÉRATION APRÈS PANNE       ");
        System.out.println("========================================\n");
        
        // 1. Lire le journal depuis le fichier
        List<LogEntry> journal = readLogFile();
        System.out.println("Journal lu: " + journal.size() + " entrées");
        
        if (journal.isEmpty()) {
            System.out.println("Journal vide, rien à récupérer.");
            return;
        }
        
        // 2. Trouver le dernier checkpoint
        int lastCheckpointIndex = -1;
        for (int i = journal.size() - 1; i >= 0; i--) {
            if (journal.get(i).type == LogEntry.LogType.CHECKPOINT) {
                lastCheckpointIndex = i;
                break;
            }
        }
        
        System.out.println("Dernier checkpoint à l'index: " + lastCheckpointIndex);
        
        // 3. Analyse : identifier les transactions committées et non committées
        Set<Integer> committedTx = new HashSet<>();
        Set<Integer> activeTx = new HashSet<>();
        
        int startIndex = (lastCheckpointIndex >= 0) ? lastCheckpointIndex : 0;
        
        for (int i = startIndex; i < journal.size(); i++) {
            LogEntry entry = journal.get(i);
            
            switch (entry.type) {
                case BEGIN:
                    activeTx.add(entry.transactionId);
                    break;
                case COMMIT:
                    activeTx.remove(entry.transactionId);
                    committedTx.add(entry.transactionId);
                    break;
                case ROLLBACK:
                    activeTx.remove(entry.transactionId);
                    break;
                default:
                    break;
            }
        }
        
        System.out.println("Transactions committées: " + committedTx);
        System.out.println("Transactions non committées (à annuler): " + activeTx);
        
        // 4. Phase REDO : rejouer les opérations des transactions committées
        System.out.println("\n--- Phase REDO ---");
        for (int i = startIndex; i < journal.size(); i++) {
            LogEntry entry = journal.get(i);
            
            if (committedTx.contains(entry.transactionId)) {
                switch (entry.type) {
                    case UPDATE:
                    case INSERT:
                        redoOperation(entry);
                        break;
                    default:
                        break;
                }
            }
        }
        
        // 5. Phase UNDO : annuler les opérations des transactions non committées
        // (parcours inverse du journal)
        System.out.println("\n--- Phase UNDO ---");
        for (int i = journal.size() - 1; i >= startIndex; i--) {
            LogEntry entry = journal.get(i);
            
            if (activeTx.contains(entry.transactionId)) {
                switch (entry.type) {
                    case UPDATE:
                    case INSERT:
                        undoOperation(entry);
                        break;
                    default:
                        break;
                }
            }
        }
        
        // 6. Forcer les pages modifiées sur disque
        System.out.println("\n--- Finalisation ---");
        for (Map.Entry<Integer, Page> entry : buffer.getAllPages().entrySet()) {
            if (entry.getValue().dirty) {
                buffer.force(entry.getKey());
                System.out.println("  [FORCE] Page " + entry.getKey() + " écrite sur disque");
            }
        }
        
        // 7. Persister le recordCount
        persistRecordCount();
        
        System.out.println("\n========================================");
        System.out.println("       RÉCUPÉRATION TERMINÉE            ");
        System.out.println("========================================\n");
    }
    
    /**
     * REDO d'une opération
     */
    private void redoOperation(LogEntry entry) throws IOException {
        if (entry.afterImage == null) return;
        
        int pageId = entry.recordId / RECORDS_PER_PAGE;
        int slot = entry.recordId % RECORDS_PER_PAGE;
        
        Page page = buffer.fix(pageId);
        System.arraycopy(entry.afterImage, 0, page.data, slot * RECORD_SIZE, RECORD_SIZE);
        buffer.use(pageId);
        buffer.unfix(pageId);
        
        // Mettre à jour recordCount si INSERT
        if (entry.type == LogEntry.LogType.INSERT) {
            if (entry.recordId >= recordCount) {
                recordCount = entry.recordId + 1;
            }
        }
        
        System.out.println("  [REDO] " + entry.type + " sur record " + entry.recordId);
    }
    
    /**
     * UNDO d'une opération
     */
    private void undoOperation(LogEntry entry) throws IOException {
        int pageId = entry.recordId / RECORDS_PER_PAGE;
        int slot = entry.recordId % RECORDS_PER_PAGE;
        
        if (entry.type == LogEntry.LogType.UPDATE && entry.beforeImage != null) {
            // Restaurer l'image avant
            Page page = buffer.fix(pageId);
            System.arraycopy(entry.beforeImage, 0, page.data, slot * RECORD_SIZE, RECORD_SIZE);
            buffer.use(pageId);
            buffer.unfix(pageId);
            System.out.println("  [UNDO] UPDATE sur record " + entry.recordId + " (restauration image avant)");
        } else if (entry.type == LogEntry.LogType.INSERT) {
            // Annuler l'insertion en décrémentant recordCount
            if (entry.recordId == recordCount - 1) {
                recordCount--;
                System.out.println("  [UNDO] INSERT sur record " + entry.recordId + " (décrémentation recordCount)");
            }
        }
    }
    
    /**
     * Affiche le contenu du journal
     */
    public void printLog() throws IOException, ClassNotFoundException {
        System.out.println("\n=== Journal de Transactions (FJT) ===");
        List<LogEntry> entries = readLogFile();
        for (LogEntry entry : entries) {
            System.out.println("  " + entry);
        }
        System.out.println("=====================================\n");
    }
    
    /**
     * Ferme les fichiers
     */
    @Override
    public void close() throws IOException {
        if (!TJT.isEmpty()) {
            flushLog();
        }
        logFile.close();
        super.close();
    }
    
    // =========================================
    // MAIN : Tests de démonstration
    // =========================================
    
    public static void main(String[] args) throws IOException {
        try {
            runTests();
        } catch (ClassNotFoundException e) {
            System.err.println("Erreur de classe: " + e.getMessage());
        }
    }
    
    private static void runTests() throws IOException, ClassNotFoundException {
        // Nettoyage des fichiers existants
        new File("td7_data.db").delete();
        new File("td7_data.db.log").delete();
        
        System.out.println("╔════════════════════════════════════════════════════════╗");
        System.out.println("║  TD7 - Journalisation, Checkpoints et Récupération     ║");
        System.out.println("╚════════════════════════════════════════════════════════╝\n");
        
        // ==========================================
        // SCÉNARIO 1 : Opérations normales + Checkpoint
        // ==========================================
        System.out.println("═══════════════════════════════════════════════════════════");
        System.out.println("  SCÉNARIO 1 : Opérations normales avec Checkpoint         ");
        System.out.println("═══════════════════════════════════════════════════════════\n");
        
        TD7 db = new TD7("td7_data.db");
        
        // Insertions hors transaction
        System.out.println("--- Insertions initiales (hors transaction) ---");
        db.insertRecord("Alice Martin");
        db.insertRecord("Bob Dupont");
        db.insertRecord("Charlie Brown");
        
        // Transaction 1 : UPDATE + COMMIT
        System.out.println("\n--- Transaction 1 : UPDATE + COMMIT ---");
        db.begin();
        db.updateRecord(1, "Robert Dupont (modifié)");
        db.commit();
        
        // Checkpoint
        db.checkpoint();
        
        // Transaction 2 : UPDATE + COMMIT
        System.out.println("--- Transaction 2 : UPDATE + COMMIT ---");
        db.begin();
        db.updateRecord(0, "Alice Martin-Dupont");
        db.commit();
        
        // Affichage du journal
        db.printLog();
        
        // Affichage des données
        System.out.println("Contenu de la base:");
        for (int i = 0; i < db.getRecordCount(); i++) {
            System.out.println("  Record " + i + ": " + db.readRecord(i));
        }
        
        db.close();
        
        // ==========================================
        // SCÉNARIO 2 : Crash et Récupération
        // ==========================================
        System.out.println("\n═══════════════════════════════════════════════════════════");
        System.out.println("  SCÉNARIO 2 : Simulation de Crash et Récupération         ");
        System.out.println("═══════════════════════════════════════════════════════════\n");
        
        // Supprimer les anciens fichiers pour un test propre
        new File("td7_data.db").delete();
        new File("td7_data.db.log").delete();
        
        db = new TD7("td7_data.db");
        
        // Insertions + Transaction committée
        System.out.println("--- Insertions initiales ---");
        db.insertRecord("Utilisateur A");
        db.insertRecord("Utilisateur B");
        db.insertRecord("Utilisateur C");
        
        // Checkpoint après insertions
        db.checkpoint();
        
        // Transaction committée (sera rejouée)
        System.out.println("--- Transaction COMMITTÉE (sera rejouée) ---");
        db.begin();
        db.updateRecord(0, "Utilisateur A MODIFIÉ");
        db.commit();
        
        // Transaction NON committée (sera annulée)
        System.out.println("\n--- Transaction NON COMMITTÉE (sera annulée) ---");
        db.begin();
        db.updateRecord(1, "Utilisateur B MODIFIÉ");
        db.insertRecord("Utilisateur D (insert non commité)");
        
        System.out.println("\nÉtat avant crash:");
        for (int i = 0; i < db.getRecordCount(); i++) {
            System.out.println("  Record " + i + ": " + db.readRecord(i));
        }
        
        // CRASH !
        db.crash();
        
        // Affichage du journal (intact après crash)
        db.printLog();
        
        // RECOVERY
        db.recover();
        
        System.out.println("État après récupération:");
        for (int i = 0; i < db.getRecordCount(); i++) {
            System.out.println("  Record " + i + ": " + db.readRecord(i));
        }
        
        db.close();
        
        // ==========================================
        // SCÉNARIO 3 : Vérification de la persistance
        // ==========================================
        System.out.println("\n═══════════════════════════════════════════════════════════");
        System.out.println("  SCÉNARIO 3 : Vérification après redémarrage              ");
        System.out.println("═══════════════════════════════════════════════════════════\n");
        
        db = new TD7("td7_data.db");
        
        System.out.println("Contenu après redémarrage (lecture depuis disque):");
        for (int i = 0; i < db.getRecordCount(); i++) {
            System.out.println("  Record " + i + ": " + db.readRecord(i));
        }
        
        db.close();
        
        System.out.println("\n╔════════════════════════════════════════════════════════╗");
        System.out.println("║              FIN DES TESTS TD7                         ║");
        System.out.println("╚════════════════════════════════════════════════════════╝");
    }
}
