package fyi.foobar.nytcorpus.index;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import com.nytlabs.corpus.NYTCorpusDocument;
import com.nytlabs.corpus.NYTCorpusDocumentParser;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

/**
 * Index NYT Corpus into Elasticsearch.
 * 
 * @author Stuart Mackie (stuart@foobar.fyi).
 * @version May 2020.
 */
public class Elastic {

    private static final Logger logger = LogManager.getLogger(Elastic.class);

    public static void index(Configuration config) {

        /*
         * Elastic:
         */

        String host = config.getString("nytcorpus.elastic.host");
        int port = config.getInt("nytcorpus.elastic.port");

        RestHighLevelClient elastic =
                new RestHighLevelClient(RestClient.builder(new HttpHost(host, port, "http")));

        /*
         * Schema:
         */

        try {

            if (elastic.indices().exists(new GetIndexRequest("nytcorpus"),
                    RequestOptions.DEFAULT)) {
                AcknowledgedResponse delete_index_response = elastic.indices()
                        .delete(new DeleteIndexRequest("nytcorpus"), RequestOptions.DEFAULT);
                logger.info("DeleteIndexRequest: " + delete_index_response.isAcknowledged());
            }

            CreateIndexRequest create_index_request = new CreateIndexRequest("nytcorpus");

            XContentBuilder schema = XContentFactory.jsonBuilder();
            schema.startObject();
            {
                schema.startObject("properties");
                schema.startObject("alternateURL").field("type", "keyword").endObject();
                schema.startObject("articleAbstract").field("type", "text").endObject();
                schema.startObject("authorBiography").field("type", "text").endObject();
                schema.startObject("banner").field("type", "keyword").endObject();
                schema.startObject("biographicalCategories").field("type", "keyword").endObject();
                schema.startObject("body").field("type", "text").endObject();
                schema.startObject("byline").field("type", "keyword").endObject();
                schema.startObject("columnName").field("type", "keyword").endObject();
                schema.startObject("columnNumber").field("type", "keyword").endObject();
                schema.startObject("correctionDate").field("type", "date").endObject();
                schema.startObject("correctionText").field("type", "text").endObject();
                schema.startObject("credit").field("type", "keyword").endObject();
                schema.startObject("dateline").field("type", "text").endObject();
                schema.startObject("dayOfWeek").field("type", "keyword").endObject();
                schema.startObject("descriptors").field("type", "keyword").endObject();
                schema.startObject("featurePage").field("type", "keyword").endObject();
                schema.startObject("generalOnlineDescriptors").field("type", "keyword").endObject();
                schema.startObject("headline").field("type", "text").endObject();
                schema.startObject("kicker").field("type", "keyword").endObject();
                schema.startObject("leadParagraph").field("type", "text").endObject();
                schema.startObject("locations").field("type", "keyword").endObject();
                schema.startObject("names").field("type", "keyword").endObject();
                schema.startObject("newsDesk").field("type", "keyword").endObject();
                schema.startObject("normalizedByline").field("type", "keyword").endObject();
                schema.startObject("onlineDescriptors").field("type", "keyword").endObject();
                schema.startObject("onlineHeadline").field("type", "text").endObject();
                schema.startObject("onlineLeadParagraph").field("type", "text").endObject();
                schema.startObject("onlineLocations").field("type", "keyword").endObject();
                schema.startObject("onlineOrganizations").field("type", "keyword").endObject();
                schema.startObject("onlinePeople").field("type", "keyword").endObject();
                schema.startObject("onlineSection").field("type", "keyword").endObject();
                schema.startObject("onlineTitles").field("type", "text").endObject();
                schema.startObject("organizations").field("type", "keyword").endObject();
                schema.startObject("page").field("type", "keyword").endObject();
                schema.startObject("people").field("type", "keyword").endObject();
                schema.startObject("publicationDate").field("type", "date").endObject();
                schema.startObject("publicationDayOfMonth").field("type", "keyword").endObject();
                schema.startObject("publicationMonth").field("type", "keyword").endObject();
                schema.startObject("publicationYear").field("type", "keyword").endObject();
                schema.startObject("section").field("type", "keyword").endObject();
                schema.startObject("seriesName").field("type", "keyword").endObject();
                schema.startObject("slug").field("type", "keyword").endObject();
                schema.startObject("sourceFile").field("type", "keyword").endObject();
                schema.startObject("taxonomicClassifiers").field("type", "keyword").endObject();
                schema.startObject("titles").field("type", "text").endObject();
                schema.startObject("typesOfMaterial").field("type", "keyword").endObject();
                schema.startObject("url").field("type", "keyword").endObject();
                schema.startObject("wordCount").field("type", "integer").endObject();
                schema.endObject();
            }
            schema.endObject();

            create_index_request.mapping(schema);
            CreateIndexResponse create_index_response =
                    elastic.indices().create(create_index_request, RequestOptions.DEFAULT);
            logger.info("CreateIndexRequest: " + create_index_response.isAcknowledged());

        } catch (IOException e) {
            logger.error(e);
            System.exit(-1);
        }

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

                                // Elastic document:
                                Map<String, Object> doc = new HashMap<String, Object>();

                                String docid = String.valueOf(nytdoc.getGuid());

                                doc.put("alternateURL", nytdoc.getAlternateURL() == null ? null
                                        : nytdoc.getAlternateURL().toString());
                                doc.put("articleAbstract",
                                        nytdoc.getArticleAbstract() == null ? null
                                                : nytdoc.getArticleAbstract());
                                doc.put("authorBiography",
                                        nytdoc.getAuthorBiography() == null ? null
                                                : nytdoc.getAuthorBiography());
                                doc.put("banner",
                                        nytdoc.getBanner() == null ? null : nytdoc.getBanner());
                                doc.put("biographicalCategories",
                                        nytdoc.getBiographicalCategories() == null ? null
                                                : nytdoc.getBiographicalCategories());
                                doc.put("body", nytdoc.getBody() == null ? null : nytdoc.getBody());
                                doc.put("byline",
                                        nytdoc.getByline() == null ? null : nytdoc.getByline());
                                doc.put("columnName", nytdoc.getColumnName() == null ? null
                                        : nytdoc.getColumnName());
                                doc.put("columnNumber", nytdoc.getColumnNumber() == null ? null
                                        : nytdoc.getColumnNumber());
                                doc.put("correctionDate", nytdoc.getCorrectionDate() == null ? null
                                        : nytdoc.getCorrectionDate());
                                doc.put("correctionText", nytdoc.getCorrectionText() == null ? null
                                        : nytdoc.getCorrectionText());
                                doc.put("credit",
                                        nytdoc.getCredit() == null ? null : nytdoc.getCredit());
                                doc.put("dateline",
                                        nytdoc.getDateline() == null ? null : nytdoc.getDateline());
                                doc.put("dayOfWeek", nytdoc.getDayOfWeek() == null ? null
                                        : nytdoc.getDayOfWeek());
                                doc.put("descriptors", nytdoc.getDescriptors() == null ? null
                                        : nytdoc.getDescriptors());
                                doc.put("featurePage", nytdoc.getFeaturePage() == null ? null
                                        : nytdoc.getFeaturePage());
                                doc.put("generalOnlineDescriptors",
                                        nytdoc.getGeneralOnlineDescriptors() == null ? null
                                                : nytdoc.getGeneralOnlineDescriptors());
                                doc.put("headline",
                                        nytdoc.getHeadline() == null ? null : nytdoc.getHeadline());
                                doc.put("kicker",
                                        nytdoc.getKicker() == null ? null : nytdoc.getKicker());
                                doc.put("leadParagraph", nytdoc.getLeadParagraph() == null ? null
                                        : nytdoc.getLeadParagraph());
                                doc.put("locations", nytdoc.getLocations() == null ? null
                                        : nytdoc.getLocations());
                                doc.put("names",
                                        nytdoc.getNames() == null ? null : nytdoc.getNames());
                                doc.put("newsDesk",
                                        nytdoc.getNewsDesk() == null ? null : nytdoc.getNewsDesk());
                                doc.put("normalizedByline",
                                        nytdoc.getNormalizedByline() == null ? null
                                                : nytdoc.getNormalizedByline());
                                doc.put("onlineDescriptors",
                                        nytdoc.getOnlineDescriptors() == null ? null
                                                : nytdoc.getOnlineDescriptors());
                                doc.put("onlineHeadline", nytdoc.getOnlineHeadline() == null ? null
                                        : nytdoc.getOnlineHeadline());
                                doc.put("onlineLeadParagraph",
                                        nytdoc.getOnlineLeadParagraph() == null ? null
                                                : nytdoc.getOnlineLeadParagraph());
                                doc.put("onlineLocations",
                                        nytdoc.getOnlineLocations() == null ? null
                                                : nytdoc.getOnlineLocations());
                                doc.put("onlineOrganizations",
                                        nytdoc.getOnlineOrganizations() == null ? null
                                                : nytdoc.getOnlineOrganizations());
                                doc.put("onlinePeople", nytdoc.getOnlinePeople() == null ? null
                                        : nytdoc.getOnlinePeople());
                                doc.put("onlineSection", nytdoc.getOnlineSection() == null ? null
                                        : nytdoc.getOnlineSection());
                                doc.put("onlineTitles", nytdoc.getOnlineTitles() == null ? null
                                        : nytdoc.getOnlineTitles());
                                doc.put("organizations", nytdoc.getOrganizations() == null ? null
                                        : nytdoc.getOrganizations());
                                doc.put("page", nytdoc.getPage() == null ? null : nytdoc.getPage());
                                doc.put("people",
                                        nytdoc.getPeople() == null ? null : nytdoc.getPeople());
                                doc.put("publicationDate",
                                        nytdoc.getPublicationDate() == null ? null
                                                : nytdoc.getPublicationDate());
                                doc.put("publicationDayOfMonth",
                                        nytdoc.getPublicationDayOfMonth() == null ? null
                                                : nytdoc.getPublicationDayOfMonth());
                                doc.put("publicationMonth",
                                        nytdoc.getPublicationMonth() == null ? null
                                                : nytdoc.getPublicationMonth());
                                doc.put("publicationYear",
                                        nytdoc.getPublicationYear() == null ? null
                                                : nytdoc.getPublicationYear());
                                doc.put("section",
                                        nytdoc.getSection() == null ? null : nytdoc.getSection());
                                doc.put("seriesName", nytdoc.getSeriesName() == null ? null
                                        : nytdoc.getSeriesName());
                                doc.put("slug", nytdoc.getSlug() == null ? null : nytdoc.getSlug());
                                doc.put("sourceFile", nytdoc.getSourceFile() == null ? null
                                        : nytdoc.getSourceFile().toString());
                                doc.put("taxonomicClassifiers",
                                        nytdoc.getTaxonomicClassifiers() == null ? null
                                                : nytdoc.getTaxonomicClassifiers());
                                doc.put("titles",
                                        nytdoc.getTitles() == null ? null : nytdoc.getTitles());
                                doc.put("typesOfMaterial",
                                        nytdoc.getTypesOfMaterial() == null ? null
                                                : nytdoc.getTypesOfMaterial());
                                doc.put("url", nytdoc.getUrl().toString() == null ? null
                                        : nytdoc.getUrl().toString());
                                doc.put("wordCount", nytdoc.getWordCount() == null ? null
                                        : nytdoc.getWordCount());

                                /*
                                 * Index document:
                                 */

                                IndexRequest index_request = new IndexRequest("nytcorpus");
                                index_request.id(docid);
                                index_request.source(doc);

                                if (elastic.index(index_request, RequestOptions.DEFAULT)
                                        .getResult() == Result.CREATED)
                                    logger.info("[" + ++i + "]" + " " + docid + " "
                                            + nytdoc.getHeadline());
                                else
                                    logger.error("Failed to index: " + docid);
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
            elastic.close();

        } catch (IOException e) {
            logger.error(e.getMessage());
            System.exit(-1);
        }
    }
}
