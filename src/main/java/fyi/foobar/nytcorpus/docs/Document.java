package fyi.foobar.nytcorpus.docs;

/**
 * NYT Corpus document.
 * 
 * @author Stuart Mackie (stuart@foobar.fyi).
 * @version May 2020.
 */
public class Document implements Comparable<Document> {

    private String docno;
    private String yyyymmdd;
    private String dateline;
    private String headline;
    private String leadpara;
    private String summary;
    private String fulltext;

    public Document(String docno, String yyyymmdd, String dateline, String headline,
            String leadpara, String summary, String fulltext) {
        this.docno = docno;
        this.yyyymmdd = yyyymmdd;
        this.dateline = dateline;
        this.headline = headline;
        this.leadpara = leadpara;
        this.summary = summary;
        this.fulltext = fulltext;
    }

    public int compareTo(Document o) {
        if (yyyymmdd.equals(o.yyyymmdd))
            return docno.compareTo(o.docno);
        else
            return yyyymmdd.compareTo(o.yyyymmdd);
    }

    public String docno() {
        return docno;
    }

    public String yyyymmdd() {
        return yyyymmdd;
    }

    public String dateline() {
        return dateline;
    }

    public String headline() {
        return headline;
    }

    public String leadpara() {
        return leadpara;
    }

    public String summary() {
        return summary;
    }

    public String fulltext() {
        return fulltext;
    }

    /** TREC <DOC>. */
    public String trecdoc() {
        StringBuilder sb = new StringBuilder();
        sb.append("<DOC>\n");
        sb.append("<DOCNO>" + docno + "</DOCNO>\n");
        sb.append("<YYYYMMDD>" + yyyymmdd + "</YYYYMMDD>\n");
        sb.append("<DATELINE>" + dateline + "</DATELINE>\n");
        sb.append("<HEADLINE>" + headline + "</HEADLINE>\n");
        sb.append("<LEADPARA>" + leadpara + "</LEADPARA>\n");
        sb.append("<SUMMARY>" + summary + "</SUMMARY>\n");
        sb.append(fulltext + "\n");
        sb.append("</DOC>\n\n");
        return sb.toString();
    }
}
