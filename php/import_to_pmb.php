<?php
set_time_limit(0);

$dbHost = 'localhost';
$dbName = 'pmb_db';
$dbUser = 'root';
$dbPass = 'root';

$baseDir = realpath(__DIR__ . '/../data') . DIRECTORY_SEPARATOR;

$csvFiles = [
    $baseDir . 'df_ar_final_pmb.csv',
    $baseDir . 'df_fr_final_pmb.csv'
];

// Log setup
$logFile = 'import_log.txt';
function logMessage($message) {
    global $logFile;
    file_put_contents($logFile, date('[Y-m-d H:i:s] ') . $message . "\n", FILE_APPEND);
}

// Clean and normalize matière
function cleanMatiere($matiere) {
    $matiere = preg_replace('/\x{00A0}/u', ' ', $matiere); // Replace non-breaking spaces
    $matiere = str_replace(['：', '﹕', '：'], ':', $matiere); // Normalize colons
    $matiere = preg_replace('/\s+/', ' ', $matiere); // Collapse multiple spaces
    $matiere = trim($matiere);
    $matiere = mb_convert_encoding($matiere, 'UTF-8', 'UTF-8');
    return $matiere;
}

// Check CSV files
foreach ($csvFiles as $file) {
    if (!file_exists($file)) {
        logMessage("❌ File not found: $file");
        die("❌ File not found: $file<br>");
    }
    echo "✔ Found: $file<br>";
}

try {
    $pdo = new PDO("mysql:host=$dbHost;dbname=$dbName;charset=utf8", $dbUser, $dbPass);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    $pdo->beginTransaction();

    // Prepare statements
    $stmtNotice = $pdo->prepare("INSERT INTO notices (typdoc, tit1, year, npages) VALUES ('a', ?, ?, ?)");
    $stmtExpl = $pdo->prepare("INSERT INTO exemplaires (expl_notice, expl_cb, expl_cote, expl_statut, expl_location) VALUES (?, ?, ?, 1, 1)");
    $stmtCheckExpl = $pdo->prepare("SELECT COUNT(*) FROM exemplaires WHERE expl_cb = ?");

    $stmtAuthor = $pdo->prepare("INSERT INTO authors (author_name) VALUES (?) ON DUPLICATE KEY UPDATE author_name = VALUES(author_name)");
    $stmtFindAuthor = $pdo->prepare("SELECT author_id FROM authors WHERE author_name = ?");

    $stmtPublisher = $pdo->prepare("INSERT INTO publishers (ed_name, ed_ville) VALUES (?, ?) ON DUPLICATE KEY UPDATE ed_name = VALUES(ed_name), ed_ville = VALUES(ed_ville)");
    $stmtFindPublisher = $pdo->prepare("SELECT ed_id FROM publishers WHERE ed_name = ?");

    $stmtUpdatePublisherId = $pdo->prepare("UPDATE notices SET ed1_id = ? WHERE notice_id = ?");

    $stmtLinkAuthor = $pdo->prepare("INSERT INTO responsability (responsability_notice, responsability_author, responsability_fonction, responsability_type) VALUES (?, ?, '0010', 0)");

    $stmtFindCategory = $pdo->prepare("SELECT num_noeud FROM categories WHERE libelle_categorie = ?");

    $stmtLinkCategory = $pdo->prepare("
        INSERT INTO notices_categories (notcateg_notice, num_noeud, num_vedette, ordre_vedette, ordre_categorie)
        VALUES (?, ?, 0, 1, 0)
    ");

    foreach ($csvFiles as $csvFile) {
        $langue = str_contains($csvFile, 'ar') ? 'ar_AR' : 'fr_FR';

        $handle = fopen($csvFile, 'r');
        fgetcsv($handle, 1000, ","); // skip header

        while (($data = fgetcsv($handle, 1000, ",")) !== FALSE) {
            if (count($data) < 9) continue;

            [$cote, $titre, $auteur, $edition, $lieu, $annee, $nbPages, $matiere, $inventaire] = $data;

            $matiere = cleanMatiere($matiere);

            // Insert notice
            $stmtNotice->execute([$titre, $annee, $nbPages]);
            $noticeId = $pdo->lastInsertId();

            // Insert exemplaire if not duplicate
            $stmtCheckExpl->execute([$inventaire]);
            if ($stmtCheckExpl->fetchColumn() == 0) {
                $stmtExpl->execute([$noticeId, $inventaire, $cote]);
            } else {
                logMessage("⚠️ Duplicate inventory skipped: $inventaire");
            }

            // Handle authors
            $authors = explode(';', $auteur);
            foreach ($authors as $authorName) {
                $authorName = trim($authorName);
                if ($authorName) {
                    $stmtAuthor->execute([$authorName]);
                    $authorId = $pdo->lastInsertId();
                    if ($authorId == 0) {
                        $stmtFindAuthor->execute([$authorName]);
                        $authorId = $stmtFindAuthor->fetchColumn();
                    }
                    if ($authorId) {
                        $stmtLinkAuthor->execute([$noticeId, $authorId]);
                    }
                }
            }

            // Handle publisher
            if ($edition) {
                $stmtPublisher->execute([$edition, $lieu]);
                $publisherId = $pdo->lastInsertId();
                if ($publisherId == 0) {
                    $stmtFindPublisher->execute([$edition]);
                    $publisherId = $stmtFindPublisher->fetchColumn();
                }
                if ($publisherId) {
                    $stmtUpdatePublisherId->execute([$publisherId, $noticeId]);
                }
            }

            // Handle category
            if ($matiere) {
                $stmtFindCategory->execute([$matiere]);
                $categoryId = $stmtFindCategory->fetchColumn();

                if (!$categoryId) {
                    $maxNumNoeud = $pdo->query("SELECT MAX(num_noeud) FROM categories WHERE langue = '$langue'")->fetchColumn();
                    $newNumNoeud = $maxNumNoeud + 1;

                    try {
                        $stmtInsertCategory = $pdo->prepare("
                            INSERT INTO categories (num_noeud, libelle_categorie, langue, num_thesaurus, note_application, comment_public, comment_voir, index_categorie, path_word_categ, index_path_word_categ)
                            VALUES (?, ?, ?, 1, '', '', '', '', '', '')
                        ");
                        $stmtInsertCategory->execute([$newNumNoeud, $matiere, $langue]);
                        $categoryId = $newNumNoeud;
                        logMessage("✔ Inserted new category: $matiere ($langue)");
                    } catch (Exception $e) {
                        logMessage("❌ Category insert failed for [$matiere]: " . $e->getMessage());
                    }
                }

                if ($categoryId) {
                    $stmtLinkCategory->execute([$noticeId, $categoryId]);
                    logMessage("✔ Linked notice $noticeId to category $categoryId ($matiere)");
                } else {
                    logMessage("❌ Failed to insert or find category for [$matiere]");
                }
            } else {
                logMessage("❌ Empty matière for notice $noticeId — skipping category link.");
            }
        }

        fclose($handle);
    }

    $pdo->commit();
    $successMsg = "✅ Import successful: " . date('Y-m-d H:i:s');
    echo "<br>$successMsg";
    logMessage($successMsg);

} catch (Exception $e) {
    if (isset($pdo) && $pdo->inTransaction()) {
        $pdo->rollBack();
    }
    $errorMsg = "❌ Error: " . $e->getMessage();
    logMessage($errorMsg);
    die("❌ Error: " . $e->getMessage());
}
?>
