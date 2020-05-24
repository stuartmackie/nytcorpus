package fyi.foobar.nytcorpus.docs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import com.nytlabs.corpus.NYTCorpusDocument;
import com.nytlabs.corpus.NYTCorpusDocumentParser;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * "YYYY-MM.gz" bundles of TREC <DOC>s from the NYT corpus.
 * 
 * @author Stuart Mackie (stuart@foobar.fyi).
 * @version May 2020.
 */
public class TRECdocs {

    private static final Logger logger = LogManager.getLogger(TRECdocs.class);

    public static void parse(Configuration config) {

        File corpus = new File(config.getString("nytcorpus.corpus"));
        File trecdocs = new File(config.getString("nytcorpus.trecdocs"));

        try {
            FileUtils.deleteDirectory(trecdocs);
            FileUtils.forceMkdir(trecdocs);
        } catch (IOException e) {
            logger.error(e.getMessage());
            System.exit(-1);
        }

        NYTCorpusDocumentParser nytcorpus = new NYTCorpusDocumentParser();

        DateTimeFormatter dtf_yyyymm =
                DateTimeFormatter.ofPattern("yyyy-MM").withZone(ZoneId.of("UTC"));
        DateTimeFormatter dtf_yyyymmdd =
                DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.of("UTC"));

        for (File file : FileUtils.listFiles(corpus, new String[] {"tgz"}, true)) {

            List<Document> archive = new ArrayList<Document>();

            try {

                String yyyymm = null;

                TarArchiveInputStream tgz = new TarArchiveInputStream(new GzipCompressorInputStream(
                        new BufferedInputStream(new FileInputStream(file))));

                TarArchiveEntry tar = null;

                while ((tar = tgz.getNextTarEntry()) != null) {

                    if (tar.isFile()) {

                        File tmp = new File(
                                System.getProperty("java.io.tmpdir") + "/" + "nytcorpus.ntif");
                        IOUtils.copy(tgz, new FileOutputStream(tmp));

                        NYTCorpusDocument document =
                                nytcorpus.parseNYTCorpusDocumentFromFile(tmp, false);

                        if (document != null) {

                            yyyymm = dtf_yyyymm.format(document.getPublicationDate().toInstant());

                            String docno = String.valueOf(document.getGuid());

                            String yyyymmdd =
                                    dtf_yyyymmdd.format(document.getPublicationDate().toInstant());

                            String dateline = document.getDateline() == null ? "[...]"
                                    : document.getDateline();
                            dateline = dateline.replaceAll("\n", " ").replaceAll("<", " ")
                                    .replaceAll(">", " ");

                            String headline = document.getHeadline() == null ? "[...]"
                                    : document.getHeadline();
                            headline = headline.replaceAll("\n", " ").replaceAll("<", " ")
                                    .replaceAll(">", " ");

                            String leadpara = document.getLeadParagraph() == null ? "[...]"
                                    : document.getLeadParagraph();
                            leadpara = leadpara.replaceAll("\n", " ").replaceAll("<", " ")
                                    .replaceAll(">", " ");

                            String summary = document.getArticleAbstract() == null ? "[...]"
                                    : document.getArticleAbstract();
                            summary = summary.replaceAll("\n", " ").replaceAll("<", " ")
                                    .replaceAll(">", " ");

                            String fulltext = document.getBody() == null ? "[...]"
                                    : document.getBody().replaceAll("\n", " ");

                            Document trecdoc = new Document(docno, yyyymmdd, dateline, headline,
                                    leadpara, summary, fulltext);
                            archive.add(trecdoc);
                        }

                        FileUtils.deleteQuietly(tmp);
                    }
                }

                tgz.close();

                Collections.sort(archive);

                File tmp = new File(System.getProperty("java.io.tmpdir") + "/" + "nytcorpus-gzip");
                FileUtils.write(tmp, "", StandardCharsets.UTF_8, false);

                for (Document trecdoc : archive)
                    FileUtils.writeStringToFile(tmp, trecdoc.trecdoc(), StandardCharsets.UTF_8,
                            true);

                FileInputStream fis = new FileInputStream(tmp);
                FileOutputStream fos = new FileOutputStream(trecdocs + "/" + yyyymm + ".gz");
                GzipCompressorOutputStream gz = new GzipCompressorOutputStream(fos);

                IOUtils.copy(fis, gz);

                gz.close();
                fos.close();
                fis.close();

                FileUtils.deleteQuietly(tmp);

            } catch (IOException e) {
                logger.error(e.getMessage());
                System.exit(-1);
            }

            logger.info(file + " " + archive.size());
        }
    }
}
