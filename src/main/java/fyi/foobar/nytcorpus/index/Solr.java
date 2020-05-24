package fyi.foobar.nytcorpus.index;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import com.nytlabs.corpus.NYTCorpusDocument;
import com.nytlabs.corpus.NYTCorpusDocumentParser;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

/**
 * Index NYT Corpus into Solr.
 * 
 * @author Stuart Mackie (stuart@foobar.fyi).
 * @version May 2020.
 */
public class Solr {

    private static final Logger logger = LogManager.getLogger(Solr.class);

    public static void index(Configuration config) {

        /*
         * Solr:
         */

        String solrhost = config.getString("nytcorpus.solr.host");
        String solrport = config.getString("nytcorpus.solr.port");

        SolrClient solr =
                new HttpSolrClient.Builder(solrhost + ":" + solrport + "/solr/" + "nytcorpus")
                        .build();

        /*
         * NYT Corpus:
         */

        String corpus = config.getString("nytcorpus.corpus");

        try {

            // Tally:
            int i = 0;

            NYTCorpusDocumentParser nytcorpus = new NYTCorpusDocumentParser();

            for (File file : FileUtils.listFiles(new File(corpus), new String[] {"tgz"}, true)) {

                try {

                    TarArchiveInputStream tgz =
                            new TarArchiveInputStream(new GzipCompressorInputStream(
                                    new BufferedInputStream(new FileInputStream(file))));

                    TarArchiveEntry tar = null;

                    while ((tar = tgz.getNextTarEntry()) != null) {

                        if (tar.isFile()) {

                            File tmp = new File(
                                    System.getProperty("java.io.tmpdir") + "/" + "nytcorpus-ntif");
                            IOUtils.copy(tgz, new FileOutputStream(tmp));

                            NYTCorpusDocument nytdoc =
                                    nytcorpus.parseNYTCorpusDocumentFromFile(tmp, false);

                            if (nytdoc != null) {

                                // Solr document:
                                SolrInputDocument doc = new SolrInputDocument();

                                String docid = String.valueOf(nytdoc.getGuid());

                                doc.addField("guid", docid);

                                doc.addField("alternateURL", nytdoc.getAlternateURL() == null ? null
                                        : nytdoc.getAlternateURL().toString());
                                doc.addField("articleAbstract",
                                        nytdoc.getArticleAbstract() == null ? null
                                                : nytdoc.getArticleAbstract());
                                doc.addField("authorBiography",
                                        nytdoc.getAuthorBiography() == null ? null
                                                : nytdoc.getAuthorBiography());
                                doc.addField("banner",
                                        nytdoc.getBanner() == null ? null : nytdoc.getBanner());
                                doc.addField("biographicalCategories",
                                        nytdoc.getBiographicalCategories() == null ? null
                                                : nytdoc.getBiographicalCategories());
                                doc.addField("body",
                                        nytdoc.getBody() == null ? null : nytdoc.getBody());
                                doc.addField("byline",
                                        nytdoc.getByline() == null ? null : nytdoc.getByline());
                                doc.addField("columnName", nytdoc.getColumnName() == null ? null
                                        : nytdoc.getColumnName());
                                doc.addField("columnNumber", nytdoc.getColumnNumber() == null ? null
                                        : nytdoc.getColumnNumber());
                                doc.addField("correctionDate",
                                        nytdoc.getCorrectionDate() == null ? null
                                                : nytdoc.getCorrectionDate());
                                doc.addField("correctionText",
                                        nytdoc.getCorrectionText() == null ? null
                                                : nytdoc.getCorrectionText());
                                doc.addField("credit",
                                        nytdoc.getCredit() == null ? null : nytdoc.getCredit());
                                doc.addField("dateline",
                                        nytdoc.getDateline() == null ? null : nytdoc.getDateline());
                                doc.addField("dayOfWeek", nytdoc.getDayOfWeek() == null ? null
                                        : nytdoc.getDayOfWeek());
                                doc.addField("descriptors", nytdoc.getDescriptors() == null ? null
                                        : nytdoc.getDescriptors());
                                doc.addField("featurePage", nytdoc.getFeaturePage() == null ? null
                                        : nytdoc.getFeaturePage());
                                doc.addField("generalOnlineDescriptors",
                                        nytdoc.getGeneralOnlineDescriptors() == null ? null
                                                : nytdoc.getGeneralOnlineDescriptors());
                                doc.addField("headline",
                                        nytdoc.getHeadline() == null ? null : nytdoc.getHeadline());
                                doc.addField("kicker",
                                        nytdoc.getKicker() == null ? null : nytdoc.getKicker());
                                doc.addField("leadParagraph",
                                        nytdoc.getLeadParagraph() == null ? null
                                                : nytdoc.getLeadParagraph());
                                doc.addField("locations", nytdoc.getLocations() == null ? null
                                        : nytdoc.getLocations());
                                doc.addField("names",
                                        nytdoc.getNames() == null ? null : nytdoc.getNames());
                                doc.addField("newsDesk",
                                        nytdoc.getNewsDesk() == null ? null : nytdoc.getNewsDesk());
                                doc.addField("normalizedByline",
                                        nytdoc.getNormalizedByline() == null ? null
                                                : nytdoc.getNormalizedByline());
                                doc.addField("onlineDescriptors",
                                        nytdoc.getOnlineDescriptors() == null ? null
                                                : nytdoc.getOnlineDescriptors());
                                doc.addField("onlineHeadline",
                                        nytdoc.getOnlineHeadline() == null ? null
                                                : nytdoc.getOnlineHeadline());
                                doc.addField("onlineLeadParagraph",
                                        nytdoc.getOnlineLeadParagraph() == null ? null
                                                : nytdoc.getOnlineLeadParagraph());
                                doc.addField("onlineLocations",
                                        nytdoc.getOnlineLocations() == null ? null
                                                : nytdoc.getOnlineLocations());
                                doc.addField("onlineOrganizations",
                                        nytdoc.getOnlineOrganizations() == null ? null
                                                : nytdoc.getOnlineOrganizations());
                                doc.addField("onlinePeople", nytdoc.getOnlinePeople() == null ? null
                                        : nytdoc.getOnlinePeople());
                                doc.addField("onlineSection",
                                        nytdoc.getOnlineSection() == null ? null
                                                : nytdoc.getOnlineSection());
                                doc.addField("onlineTitles", nytdoc.getOnlineTitles() == null ? null
                                        : nytdoc.getOnlineTitles());
                                doc.addField("organizations",
                                        nytdoc.getOrganizations() == null ? null
                                                : nytdoc.getOrganizations());
                                doc.addField("page",
                                        nytdoc.getPage() == null ? null : nytdoc.getPage());
                                doc.addField("people",
                                        nytdoc.getPeople() == null ? null : nytdoc.getPeople());
                                doc.addField("publicationDate",
                                        nytdoc.getPublicationDate() == null ? null
                                                : nytdoc.getPublicationDate());
                                doc.addField("publicationDayOfMonth",
                                        nytdoc.getPublicationDayOfMonth() == null ? null
                                                : nytdoc.getPublicationDayOfMonth());
                                doc.addField("publicationMonth",
                                        nytdoc.getPublicationMonth() == null ? null
                                                : nytdoc.getPublicationMonth());
                                doc.addField("publicationYear",
                                        nytdoc.getPublicationYear() == null ? null
                                                : nytdoc.getPublicationYear());
                                doc.addField("section",
                                        nytdoc.getSection() == null ? null : nytdoc.getSection());
                                doc.addField("seriesName", nytdoc.getSeriesName() == null ? null
                                        : nytdoc.getSeriesName());
                                doc.addField("slug",
                                        nytdoc.getSlug() == null ? null : nytdoc.getSlug());
                                doc.addField("sourceFile", nytdoc.getSourceFile() == null ? null
                                        : nytdoc.getSourceFile().toString());
                                doc.addField("taxonomicClassifiers",
                                        nytdoc.getTaxonomicClassifiers() == null ? null
                                                : nytdoc.getTaxonomicClassifiers());
                                doc.addField("titles",
                                        nytdoc.getTitles() == null ? null : nytdoc.getTitles());
                                doc.addField("typesOfMaterial",
                                        nytdoc.getTypesOfMaterial() == null ? null
                                                : nytdoc.getTypesOfMaterial());
                                doc.addField("url", nytdoc.getUrl().toString() == null ? null
                                        : nytdoc.getUrl().toString());
                                doc.addField("wordCount", nytdoc.getWordCount() == null ? null
                                        : nytdoc.getWordCount());

                                /*
                                 * Index document:
                                 */

                                solr.add(doc);

                                logger.info(
                                        "[" + ++i + "]" + " " + docid + " " + nytdoc.getHeadline());

                            }

                            FileUtils.deleteQuietly(tmp);
                        }
                    }

                    tgz.close();

                } catch (IOException e) {
                    logger.error(e.getMessage());
                    System.exit(-1);
                }

            }

            // Tidy up:
            solr.commit();
            solr.close();

        } catch (SolrServerException | IOException e) {
            logger.error(e.getMessage());
            System.exit(-1);
        }
    }
}
