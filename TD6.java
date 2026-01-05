import java.io.*;
import java.util.*;

/**
 * TD6 - TIV, TIA et mécanisme de verrouillage
 * 
 * Cette classe étend TD3 pour ajouter :
 * - TIV (Tampon d'Images Avant) : sauvegarde des pages avant modification
 * - Mécanisme de verrouillage des enregistrements
 * - Politique de lecture cohérente (lecture depuis TIV si verrou)
 * - ROLLBACK réel avec restauration depuis TIV
 * - COMMIT avec libération des verrous et nettoyage du TIV
 */
public class TD6 extends TD3 {
    
    // TIV : Tampon d'Images Avant - stocke les pages originales avant modification
    // Clé = pageId, Valeur = copie des données de la page avant modification
    private Map<Integer, byte[]> TIV = new HashMap<>();
    
    // Table de verrous : ensemble des enregistrements verrouillés
    // Un RecordId est représenté par un identifiant unique (pageId * RECORDS_PER_PAGE + slot)
    private Set<Integer> locks = new HashSet<>();
    
    // Identifiant de la transaction courante (pour gérer plusieurs transactions)
    private int currentTransactionId = 0;
    
    // Map pour savoir quelle transaction possède quel verrou
    private Map<Integer, Integer> lockOwners = new HashMap<>();

    public TD6(String filename) throws IOException {
        super(filename);
    }

    /**
     * Calcule l'identifiant unique d'un enregistrement (RecordId)
     */
    private int getRecordId(int pageId, int slot) {
        return pageId * RECORDS_PER_PAGE + slot;
    }

    /**
     * Vérifie si un enregistrement est verrouillé
     */
    public boolean isLocked(int recordId) {
        return locks.contains(recordId);
    }

    /**
     * Vérifie si un enregistrement est verrouillé par la transaction courante
     */
    private boolean isLockedByCurrentTransaction(int recordId) {
        Integer owner = lockOwners.get(recordId);
        return owner != null && owner == currentTransactionId;
    }

    /**
     * Pose un verrou sur un enregistrement pour la transaction courante
     */
    private void acquireLock(int recordId) {
        locks.add(recordId);
        lockOwners.put(recordId, currentTransactionId);
    }

    /**
     * Libère un verrou sur un enregistrement
     */
    private void releaseLock(int recordId) {
        locks.remove(recordId);
        lockOwners.remove(recordId);
    }

    /**
     * Sauvegarde l'image avant d'une page dans le TIV (si pas déjà fait)
     */
    private void saveToTIV(int pageId) throws IOException {
        if (!TIV.containsKey(pageId)) {
            Page page = buffer.fix(pageId);
            // Copie profonde des données de la page
            byte[] imageBefore = Arrays.copyOf(page.data, page.data.length);
            TIV.put(pageId, imageBefore);
            buffer.unfix(pageId);
            System.out.println("  [TIV] Sauvegarde de l'image avant pour la page " + pageId);
        }
    }

    /**
     * BEGIN - Démarre une nouvelle transaction
     */
    @Override
    public void begin() throws IOException {
        if (inTransaction) commit();
        inTransaction = true;
        currentTransactionId++;
        recordCountBeforeTransaction = recordCount;
        System.out.println("BEGIN (Transaction #" + currentTransactionId + ")");
    }

    /**
     * COMMIT - Valide la transaction
     * 1. Force sur disque les pages transactionnelles modifiées
     * 2. Libère tous les verrous
     * 3. Vide le TIV
     */
    @Override
    public void commit() throws IOException {
        if (!inTransaction) return;
        
        System.out.println("COMMIT (Transaction #" + currentTransactionId + ")");
        
        // 1. Force toutes les pages transactionnelles sur disque
        for (Map.Entry<Integer, Page> entry : buffer.getAllPages().entrySet()) {
            Page page = entry.getValue();
            if (page.dirty && page.transactional) {
                buffer.force(entry.getKey());
                page.transactional = false;
                System.out.println("  [FORCE] Page " + entry.getKey() + " écrite sur disque");
            }
        }
        
        // 2. Libère tous les verrous de cette transaction
        List<Integer> locksToRelease = new ArrayList<>();
        for (Map.Entry<Integer, Integer> entry : lockOwners.entrySet()) {
            if (entry.getValue() == currentTransactionId) {
                locksToRelease.add(entry.getKey());
            }
        }
        for (int recordId : locksToRelease) {
            releaseLock(recordId);
            System.out.println("  [UNLOCK] Libération du verrou sur l'enregistrement " + recordId);
        }
        
        // 3. Vide le TIV
        TIV.clear();
        System.out.println("  [TIV] Nettoyage du tampon d'images avant");
        
        inTransaction = false;
    }

    /**
     * ROLLBACK - Annule la transaction
     * 1. Restaure les pages depuis le TIV
     * 2. Libère tous les verrous
     * 3. Vide le TIV
     */
    @Override
    public void rollback() {
        if (!inTransaction) return;
        
        System.out.println("ROLLBACK (Transaction #" + currentTransactionId + ")");
        
        // 1. Restaure les pages depuis le TIV vers le TIA (buffer)
        for (Map.Entry<Integer, byte[]> entry : TIV.entrySet()) {
            int pageId = entry.getKey();
            byte[] imageBefore = entry.getValue();
            
            Page page = buffer.getAllPages().get(pageId);
            if (page != null) {
                // Restaure les données originales
                System.arraycopy(imageBefore, 0, page.data, 0, imageBefore.length);
                page.dirty = false;  // La page redevient propre (identique au disque)
                page.transactional = false;
                System.out.println("  [RESTORE] Page " + pageId + " restaurée depuis TIV");
            }
        }
        
        // 2. Libère tous les verrous de cette transaction
        List<Integer> locksToRelease = new ArrayList<>();
        for (Map.Entry<Integer, Integer> entry : lockOwners.entrySet()) {
            if (entry.getValue() == currentTransactionId) {
                locksToRelease.add(entry.getKey());
            }
        }
        for (int recordId : locksToRelease) {
            releaseLock(recordId);
            System.out.println("  [UNLOCK] Libération du verrou sur l'enregistrement " + recordId);
        }
        
        // 3. Vide le TIV
        TIV.clear();
        System.out.println("  [TIV] Nettoyage du tampon d'images avant");
        
        // 4. Restaure le compteur d'enregistrements
        recordCount = recordCountBeforeTransaction;
        
        inTransaction = false;
    }

    /**
     * Mise à jour d'un enregistrement existant avec gestion des verrous
     */
    public void updateRecord(int id, String newData) throws IOException {
        if (id < 0 || id >= recordCount) {
            throw new IOException("Record " + id + " n'existe pas !");
        }
        
        int pageId = id / RECORDS_PER_PAGE;
        int slot = id % RECORDS_PER_PAGE;
        int recordId = getRecordId(pageId, slot);
        
        // 1. Vérifier si l'enregistrement est déjà verrouillé par une autre transaction
        if (isLocked(recordId) && !isLockedByCurrentTransaction(recordId)) {
            throw new IOException("Enregistrement " + id + " verrouillé par une autre transaction !");
        }
        
        // 2. Si pas encore verrouillé, acquérir le verrou et sauvegarder dans TIV
        if (!isLockedByCurrentTransaction(recordId)) {
            // Sauvegarder l'image avant de la page complète dans le TIV
            saveToTIV(pageId);
            // Poser le verrou
            acquireLock(recordId);
            System.out.println("  [LOCK] Verrou posé sur l'enregistrement " + id);
        }
        
        // 3. Modifier la donnée dans le TIA (buffer)
        Page page = buffer.fix(pageId);
        byte[] bytes = newData.getBytes("UTF-8");
        byte[] record = new byte[RECORD_SIZE];
        System.arraycopy(bytes, 0, record, 0, Math.min(bytes.length, RECORD_SIZE));
        System.arraycopy(record, 0, page.data, slot * RECORD_SIZE, RECORD_SIZE);
        
        // 4. Marquer la page comme dirty et transactionnelle
        buffer.use(pageId);
        page.transactional = true;
        buffer.unfix(pageId);
        
        System.out.println("  [UPDATE] Enregistrement " + id + " mis à jour dans TIA");
    }

    /**
     * Lecture d'un enregistrement avec politique de lecture cohérente
     * - Si pas de transaction ou pas de verrou : lecture depuis TIA
     * - Si verrou posé par une autre transaction : lecture depuis TIV
     */
    @Override
    public String readRecord(int id) throws IOException {
        if (id < 0 || id >= recordCount) {
            throw new IOException("Record " + id + " n'existe pas !");
        }
        
        int pageId = id / RECORDS_PER_PAGE;
        int slot = id % RECORDS_PER_PAGE;
        int recordId = getRecordId(pageId, slot);
        
        byte[] recordData;
        
        // Politique de lecture cohérente
        if (isLocked(recordId) && !isLockedByCurrentTransaction(recordId)) {
            // Enregistrement verrouillé par une autre transaction → lecture depuis TIV
            byte[] pageData = TIV.get(pageId);
            if (pageData != null) {
                recordData = Arrays.copyOfRange(pageData, slot * RECORD_SIZE, (slot + 1) * RECORD_SIZE);
                System.out.println("  [READ] Lecture depuis TIV (verrou autre transaction)");
            } else {
                // Pas d'image avant, lecture normale depuis TIA
                Page page = buffer.fix(pageId);
                recordData = Arrays.copyOfRange(page.data, slot * RECORD_SIZE, (slot + 1) * RECORD_SIZE);
                buffer.unfix(pageId);
            }
        } else {
            // Pas de verrou ou verrou par la transaction courante → lecture depuis TIA
            Page page = buffer.fix(pageId);
            recordData = Arrays.copyOfRange(page.data, slot * RECORD_SIZE, (slot + 1) * RECORD_SIZE);
            buffer.unfix(pageId);
        }
        
        return new String(recordData, "UTF-8").trim();
    }

    /**
     * Insertion d'un enregistrement avec gestion des verrous
     */
    @Override
    public void insertRecord(String data) throws IOException {
        long totalRecords = recordCount;
        int pageId = (int) (totalRecords / RECORDS_PER_PAGE);
        int slot = (int) (totalRecords % RECORDS_PER_PAGE);
        int recordId = getRecordId(pageId, slot);
        
        if (inTransaction) {
            // Sauvegarder l'image avant de la page dans le TIV
            saveToTIV(pageId);
            // Poser un verrou sur le nouvel enregistrement
            acquireLock(recordId);
        }
        
        super.insertRecord(data);
        
        if (inTransaction) {
            System.out.println("  [LOCK] Verrou posé sur le nouvel enregistrement " + totalRecords);
        }
    }

    /**
     * Affiche l'état actuel du système (pour debug)
     */
    public void printStatus() {
        System.out.println("\n=== État du système ===");
        System.out.println("Transaction en cours: " + (inTransaction ? "Oui (#" + currentTransactionId + ")" : "Non"));
        System.out.println("Nombre d'enregistrements: " + recordCount);
        System.out.println("Pages dans TIV: " + TIV.keySet());
        System.out.println("Verrous actifs: " + locks);
        System.out.println("========================\n");
    }

    public static void main(String[] args) throws IOException {
        // Supprime le fichier existant pour un test propre
        new File("td6_data.db").delete();
        
        TD6 db = new TD6("td6_data.db");

        System.out.println("========================================");
        System.out.println("       TD6 - TIV, TIA et Verrouillage   ");
        System.out.println("========================================\n");

        // Test 1: Insertion sans transaction
        System.out.println("=== Test 1: Insertions initiales (hors transaction) ===");
        db.insertRecord("Alice Martin");
        db.insertRecord("Bob Dupont");
        db.insertRecord("Charlie Brown");
        System.out.println("3 enregistrements insérés.\n");

        // Affichage des données
        System.out.println("Contenu actuel:");
        for (int i = 0; i < db.getRecordCount(); i++) {
            System.out.println("  Record " + i + ": " + db.readRecord(i));
        }

        // Test 2: Mise à jour avec COMMIT
        System.out.println("\n=== Test 2: UPDATE avec COMMIT ===");
        db.begin();
        db.printStatus();
        
        db.updateRecord(1, "Robert Dupont (modifié)");
        db.printStatus();
        
        System.out.println("Lecture pendant transaction (par la même transaction):");
        System.out.println("  Record 1: " + db.readRecord(1));
        
        db.commit();
        
        System.out.println("\nAprès COMMIT:");
        for (int i = 0; i < db.getRecordCount(); i++) {
            System.out.println("  Record " + i + ": " + db.readRecord(i));
        }

        // Test 3: Mise à jour avec ROLLBACK
        System.out.println("\n=== Test 3: UPDATE avec ROLLBACK ===");
        db.begin();
        
        db.updateRecord(0, "Alice MODIFIEE");
        db.updateRecord(2, "Charlie MODIFIE");
        
        System.out.println("Avant ROLLBACK:");
        for (int i = 0; i < db.getRecordCount(); i++) {
            System.out.println("  Record " + i + ": " + db.readRecord(i));
        }
        
        db.printStatus();
        db.rollback();
        
        System.out.println("\nAprès ROLLBACK (valeurs restaurées depuis TIV):");
        for (int i = 0; i < db.getRecordCount(); i++) {
            System.out.println("  Record " + i + ": " + db.readRecord(i));
        }
        db.printStatus();

        // Test 4: Insertion en transaction avec ROLLBACK
        System.out.println("\n=== Test 4: INSERT en transaction avec ROLLBACK ===");
        db.begin();
        db.insertRecord("David Wilson (sera annulé)");
        db.insertRecord("Eve Johnson (sera annulé)");
        
        System.out.println("Avant ROLLBACK: " + db.getRecordCount() + " enregistrements");
        db.rollback();
        System.out.println("Après ROLLBACK: " + db.getRecordCount() + " enregistrements");

        // Test 5: Insertion en transaction avec COMMIT
        System.out.println("\n=== Test 5: INSERT en transaction avec COMMIT ===");
        db.begin();
        db.insertRecord("Frank Miller");
        db.commit();
        
        System.out.println("\nContenu final:");
        for (int i = 0; i < db.getRecordCount(); i++) {
            System.out.println("  Record " + i + ": " + db.readRecord(i));
        }

        db.close();
        System.out.println("\n=== Fin des tests TD6 ===");
    }
}
