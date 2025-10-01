import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

class MiniSGBD implements AutoCloseable {
    private static final int PAGE_SIZE = 4096;   // 4 Ko
    private static final int RECORD_SIZE = 100;  // 100 octets
    private static final int RECORDS_PER_PAGE = PAGE_SIZE / RECORD_SIZE;

    private RandomAccessFile file;

    public MiniSGBD(String filename) throws IOException {
        File f = new File(filename);
        if (f.exists()) {
            f.delete();  // supprime le fichier existant
        }
        this.file = new RandomAccessFile(filename, "rw");
    }

    public void insertRecord(String data) throws IOException {
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        byte[] buffer = new byte[RECORD_SIZE];
        int len = Math.min(bytes.length, RECORD_SIZE);
        System.arraycopy(bytes, 0, buffer, 0, len);
        file.seek(file.length());
        file.write(buffer);
    }

    public String readRecord(int id) throws IOException {
        long pos = (long) id * RECORD_SIZE;
        if (pos >= file.length()) {
            throw new IOException("Record " + id + " n'existe pas !");
        }
        file.seek(pos);
        byte[] buffer = new byte[RECORD_SIZE];
        file.readFully(buffer);
        return new String(buffer, StandardCharsets.UTF_8).trim();
    }

    public List<String> getPage(int pageId) throws IOException {
        List<String> records = new ArrayList<>();

        long startRecord = pageId * RECORDS_PER_PAGE; // position du premier enregistrement de la page
        if (startRecord >= file.length() / RECORD_SIZE) {
            return records; // page inexistante → retourne liste vide
        }

        file.seek(startRecord * RECORD_SIZE);

        // Calcul du nombre d'enregistrements à lire pour cette page
        long remainingRecords = (file.length() / RECORD_SIZE) - startRecord;
        int recordsToRead = (int) Math.min(RECORDS_PER_PAGE, remainingRecords);

        for (int i = 0; i < recordsToRead; i++) {
            byte[] buffer = new byte[RECORD_SIZE];
            file.readFully(buffer);
            records.add(new String(buffer, StandardCharsets.UTF_8).trim());
        }

        return records;
    }

    public void close() throws IOException {
        if (file != null) {
            file.close();
        }
    }
}

public class td {
    public static void main(String[] args) throws IOException {
        // Le try-with-resources s'assure que db.close() est appelé automatiquement
        try (MiniSGBD db = new MiniSGBD("etudiants.db")) {

            int totalRecords = 160; // 4 pages * 40 enregistrements/page
            System.out.println("Insertion de " + totalRecords + " enregistrements...");
            for (int i = 1; i <= totalRecords; i++) {
                db.insertRecord("Etudiant " + i);
            }
            System.out.println("Insertion terminée.\n");

            // Test de lecture d'un enregistrement spécifique
            System.out.println("Lecture de l'enregistrement 42 (ID 41) : " + db.readRecord(41));
            System.out.println("-------------------------------------\n");

            // Affichage du contenu des 4 pages
            for (int i = 0; i < 4; i++) {
                List<String> pageContent = db.getPage(i);
                System.out.println("Contenus de la Page " + i + " (Taille: " + pageContent.size() + " enregistrements)");

                // On n'affiche que le premier et le dernier pour la lisibilité
                if (!pageContent.isEmpty()) {
                    System.out.println(" -> Premier: " + pageContent.get(0));
                    System.out.println(" -> Dernier: " + pageContent.get(pageContent.size() - 1));
                }

                System.out.println();
            }

        } catch (IOException e) {
            System.err.println("Une erreur est survenue: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
