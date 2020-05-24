package fyi.foobar.nytcorpus;

import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.commons.configuration2.SystemConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import fyi.foobar.nytcorpus.docs.TRECdocs;
import fyi.foobar.nytcorpus.index.Elastic;
import fyi.foobar.nytcorpus.index.Solr;

/**
 * New York Times Annotated Corpus.
 * 
 * @author Stuart Mackie (stuart@foobar.fyi).
 * @version May 2020.
 */
public class App {

    private static final Logger logger = LogManager.getLogger(App.class);

    public static void main(String[] args) {

        /*
         * Initialisation:
         */

        logger.info("New York Times Annotated Corpus");

        CompositeConfiguration config = new CompositeConfiguration();

        try {

            // System properties (-Dfoo=bar):
            config.addConfiguration(new SystemConfiguration());

            // Application properties:
            Configurations configs = new Configurations();
            config.addConfiguration(configs.properties("application.properties"));

        } catch (ConfigurationException e) {
            logger.error("Configuration error.", e);
            System.exit(-1);
        }

        /*
         * TREC <DOC>'s:
         */

        if (args[0].equals("trecdocs"))
            TRECdocs.parse(config);

        /*
         * Index the NYT corpus into Solr:
         */

        if (args[0].equals("solr"))
            Solr.index(config);

        /*
         * Index the NYT corpus into Elasticsearch:
         */

        if (args[0].equals("elastic"))
            Elastic.index(config);

    }
}
