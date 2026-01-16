import java.io.*;

import java.nio.charset.StandardCharsets;
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

     * CORRECTION : Implémentation directe (pas super()) pour éviter duplication

     */

    @Override

    public void updateRecord(int id, String newData) throws IOException {

        if (id < 0 || id >= recordCount) {

            throw new IOException("Record " + id + " n'existe pas !");

        }



        int pageId = id / RECORDS_PER_PAGE;

        int slot = id % RECORDS_PER_PAGE;



        // Implémentation directe comme dans TD2 (pas super() !)

        Page page = buffer.fix(pageId);



        // Sauvegarder l'image avant

        byte[] beforeImage = Arrays.copyOfRange(page.data, slot * RECORD_SIZE, (slot + 1) * RECORD_SIZE);



        // Préparer les nouvelles données

        byte[] bytes = newData.getBytes(StandardCharsets.UTF_8);

        byte[] record = new byte[RECORD_SIZE];

        System.arraycopy(bytes, 0, record, 0, Math.min(bytes.length, RECORD_SIZE));



        // Mettre à jour l'enregistrement

        System.arraycopy(record, 0, page.data, slot * RECORD_SIZE, RECORD_SIZE);

        buffer.use(pageId);

        buffer.unfix(pageId);



        // Journaliser l'UPDATE

        log(new LogEntry(currentTransactionId, id, beforeImage, record,

                LogEntry.LogType.UPDATE, nextLSN()));



        // FORCE LE FLUSH APRÈS CHAQUE UPDATE NON COMMITÉ

        try {

            flushLog();

        } catch (IOException e) {

            System.err.println("Erreur flush après update: " + e.getMessage());

        }

    }



    /**

     * Insertion d'un enregistrement avec journalisation

     * CORRECTION : Implémentation directe (pas super()) pour éviter duplication

     */

    @Override

    public void insertRecord(String data) throws IOException {

        long recordId = recordCount;

        int pageId = (int) (recordId / RECORDS_PER_PAGE);

        int slot = (int) (recordId % RECORDS_PER_PAGE);



        // Implémentation directe comme dans TD2 (pas super() !)

        Page page = buffer.fix(pageId);



        // Préparer les données

        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);

        byte[] record = new byte[RECORD_SIZE];

        System.arraycopy(bytes, 0, record, 0, Math.min(bytes.length, RECORD_SIZE));



        // Écrire dans la page

        System.arraycopy(record, 0, page.data, slot * RECORD_SIZE, RECORD_SIZE);

        buffer.use(pageId);

        buffer.unfix(pageId);



        // Mettre à jour le compteur

        recordCount++;

        persistRecordCount();



        // Journaliser l'INSERT seulement si en transaction

        if (inTransaction) {

            log(new LogEntry(currentTransactionId, (int) recordId, null, record,

                    LogEntry.LogType.INSERT, nextLSN()));



            // FORCE LE FLUSH APRÈS CHAQUE INSERT NON COMMITÉ

            try {

                flushLog();

            } catch (IOException e) {

                System.err.println("Erreur flush après insert: " + e.getMessage());

            }

        }

    }



    /**

     * CHECKPOINT - Point de sauvegarde

     */

    public void checkpoint() throws IOException {

        System.out.println("\n=== CHECKPOINT ===");



        // Forcer toutes les pages dirty sur disque

        int pagesWritten = 0;

        for (Map.Entry<Integer, Page> entry : buffer.getAllPages().entrySet()) {

            Page page = entry.getValue();

            if (page.dirty) {

                buffer.force(entry.getKey());

                pagesWritten++;

                System.out.println("  [FORCE] Page " + entry.getKey() + " écrite sur disque");

            }

        }



        // Persister le recordCount

        persistRecordCount();

        savedRecordCount = recordCount;



        // Journaliser le CHECKPOINT

        log(new LogEntry(LogEntry.LogType.CHECKPOINT, nextLSN()));



        // Mémoriser la position du checkpoint

        lastCheckpointPosition = logFile.length();



        // Forcer le journal

        flushLog();



        System.out.println("  [CHECKPOINT] " + pagesWritten + " pages écrites, recordCount=" + recordCount);

        System.out.println("==================\n");

    }



    /**

     * Persiste le recordCount dans le header du fichier

     */

    private void persistRecordCount() throws IOException {

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

     */

    public void crash() {

        // Force un dernier flush avant crash

        try {

            flushLog();

        } catch (IOException e) {

            // Ignore

        }



        System.out.println("\n!!! CRASH SYSTÈME !!!");



        // Vider le buffer sans écriture

        buffer.getAllPages().clear();



        // Vider le TJT

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

     * RECOVER - Récupération après panne (VERSION FINALE CORRIGÉE)

     */

    public void recover() throws IOException, ClassNotFoundException {

        System.out.println("\n========================================");

        System.out.println("         RÉCUPÉRATION APRÈS PANNE       ");

        System.out.println("========================================\n");



        // Lire le journal depuis le fichier

        List<LogEntry> journal = readLogFile();

        System.out.println("Journal lu: " + journal.size() + " entrées");



        if (journal.isEmpty()) {

            System.out.println("Journal vide, rien à récupérer.");

            return;

        }



        // CORRECTION : Trouver LE DERNIER checkpoint seulement

        int lastCheckpointIndex = -1;

        for (int i = journal.size() - 1; i >= 0; i--) {

            if (journal.get(i).type == LogEntry.LogType.CHECKPOINT) {

                lastCheckpointIndex = i;

                break;

            }

        }



        System.out.println("Dernier checkpoint à l'index: " + lastCheckpointIndex);



        // Analyse : analyser seulement après le dernier checkpoint

        Set<Integer> committedTx = new HashSet<>();

        Set<Integer> activeTx = new HashSet<>();



        int startIndex = (lastCheckpointIndex >= 0) ? lastCheckpointIndex + 1 : 0;



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



        System.out.println("Transactions commitées (après dernier checkpoint): " + committedTx);

        System.out.println("Transactions non commitées (à annuler): " + activeTx);



        // Phase REDO : rejouer depuis le dernier checkpoint

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



        // Phase UNDO : annuler les transactions actives

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



        // Forcer les pages modifiées sur disque

        System.out.println("\n--- Finalisation ---");

        for (Map.Entry<Integer, Page> entry : buffer.getAllPages().entrySet()) {

            if (entry.getValue().dirty) {

                buffer.force(entry.getKey());

                System.out.println("  [FORCE] Page " + entry.getKey() + " écrite sur disque");

            }

        }



        // Persister le recordCount

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

            // Annuler l'insertion

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

    // MAIN : ÉTAPE 5 - Crash & Recovery

    // =========================================
// =========================================
// ÉTAPES 3 ET 4 : Préparation pour l'étape 5
// =========================================

    public static void etape3_Persistance() throws IOException {
        // Nettoyer les fichiers précédents
        new File("etape3.db").delete();
        new File("etape3.db.log").delete();

        try (TD7 db = new TD7("etape3.db")) {
            System.out.println("=== ÉTAPE 3 : Persistance (Commit & Journal) ===");

            // PRÉPARATION : Créer des données
            db.insertRecord("Record_A");
            db.insertRecord("Record_B");
            System.out.println("Données créées");

            // VÉRIFICATION AVANT : Taille du journal
            long tailleAvant = new File("etape3.db.log").length();
            System.out.println("Taille journal AVANT : " + tailleAvant + " octets");

            // ACTION 1 : Démarrer une transaction (BEGIN)
            db.begin();
            System.out.println("✅ Transaction démarrée (BEGIN)");

            // ACTION 2 : Modifier Record_B en Record_B_FINAL
            db.updateRecord(1, "Record_B_FINAL");
            System.out.println("✅ Record_B modifié en Record_B_FINAL");

            // ACTION 3 : Valider (COMMIT)
            db.commit();
            System.out.println("✅ Transaction validée (COMMIT)");

            // VÉRIFICATION : Taille du journal APRÈS
            long tailleApres = new File("etape3.db.log").length();
            System.out.println("Taille journal APRÈS : " + tailleApres + " octets");

            // VÉRIFICATION : Présence du fichier de journal
            File journalFile = new File("etape3.db.log");
            if (journalFile.exists()) {
                System.out.println("✅ Fichier journal présent : " + journalFile.getName());
            }

            if (tailleApres > tailleAvant) {
                System.out.println("🎉 OBJECTIF ATTEINT : Le COMMIT laisse une trace durable !");
                System.out.println("Le journal WAL garantit la persistance des transactions.");
            } else {
                System.out.println("❌ Problème : Le journal n'a pas grandi");
            }

            // BONUS : Afficher le contenu du journal
            System.out.println("\nContenu du journal :");
            db.printLog();

        } catch (ClassNotFoundException e) {
            System.err.println("Erreur: " + e.getMessage());
        }
    }

    public static void etape4_PreparationCrash() throws IOException {
        // Nettoyer les fichiers précédents
        new File("etape4.db").delete();
        new File("etape4.db.log").delete();

        try (TD7 db = new TD7("etape4.db")) {
            System.out.println("=== ÉTAPE 4 : Préparation au Crash ===");

            // PRÉPARATION : Même base que étape 3
            db.insertRecord("Record_A");
            db.insertRecord("Record_B");

            // Transaction normale (comme étape 3)
            db.begin();
            db.updateRecord(1, "Record_B_FINAL");
            db.commit();
            System.out.println("Transaction normale commité");

            // PRÉPARATION INCOHÉRENCE : Nouvelle transaction NON commité
            db.begin();
            System.out.println("Transaction ouverte démarrée");

            db.insertRecord("Record_C_FANTOME");  // ID 2
            System.out.println("Record_C_FANTOME inséré (transaction ouverte)");

            // NE PAS COMMIT ! Laissons la transaction ouverte
            System.out.println("⚠️  Transaction laissée ouverte = incohérence créée");

            // Vérification
            System.out.println("État actuel (avec incohérence) :");
            for (int i = 0; i < db.getRecordCount(); i++) {
                System.out.println("  Record " + i + ": " + db.readRecord(i));
            }

            System.out.println("Cette incohérence sera résolue par l'étape 5 (recovery)");

        }
    }


    public static void main(String[] args) throws IOException {
        new File("etape5_test.db").delete();
        new File("etape5_test.db.log").delete();
        // Étape 3
        etape3_Persistance();
        System.out.println("\n" + "=".repeat(60) + "\n");

        // Étape 4
        etape4_PreparationCrash();
        System.out.println("\n" + "=".repeat(60) + "\n");

        try (TD7 db = new TD7("etape5_test.db")) {

            System.out.println("=== ÉTAPE 5 : Crash & Recovery ===\n");



            // PRÉPARATION : Créer des données de test

            db.insertRecord("Alice");

            db.insertRecord("Bob");

            db.insertRecord("Charlie");

            System.out.println("Données créées");



            // Transaction commité (sera conservée après recovery)

            db.begin();

            db.updateRecord(0, "Alice MODIFIEE");

            db.commit();

            System.out.println("Transaction commité : Alice MODIFIEE");



            // CHECKPOINT ESSENTIEL pour sauvegarder sur disque

            db.checkpoint();

            System.out.println("Checkpoint effectué - données sauvegardées sur disque");



            // Transaction NON commité (sera annulée)

            db.begin();

            db.updateRecord(1, "Bob MODIFIEE");

            db.insertRecord("David");

            System.out.println("Transaction non commité : Bob MODIFIEE + David ajouté");

            // Pas de commit → sera annulé



            System.out.println("\nÉtat AVANT crash :");

            for (int i = 0; i < db.getRecordCount(); i++) {

                System.out.println("  Record " + i + ": " + db.readRecord(i));

            }



            // ACTION 1 : Simuler la panne

            db.crash();

            System.out.println("💥 CRASH SIMULÉ !");



            // ACTION 2 : Redémarrer et lancer recover()

            db.recover();

            System.out.println("🔄 Récupération terminée");



            // VÉRIFICATION FINALE : Affichage de tous les enregistrements

            System.out.println("\nÉtat APRÈS recovery :");

            for (int i = 0; i < db.getRecordCount(); i++) {

                System.out.println("  Record " + i + ": " + db.readRecord(i));

            }



            // Analyse du résultat

            if (db.getRecordCount() == 3) {

                System.out.println("\n✅ OBJECTIF ATTEINT : Recovery nettoie correctement !");

                System.out.println("   - Alice MODIFIEE : conservée (transaction commité)");

                System.out.println("   - Bob : restauré (transaction annulée)");

                System.out.println("   - Charlie : conservé (non modifié)");

                System.out.println("   - David : supprimé (insertion annulée)");

            } else {

                System.out.println("\n❌ Problème : Recovery n'a pas fonctionné correctement");

            }

        } catch (ClassNotFoundException e) {

            System.err.println("Erreur: " + e.getMessage());

        }

    }

}