import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static java.lang.System.exit;

public class StartMigration {

    static private RestClientBuilder.HttpClientConfigCallback getHttpConfig(CredentialsProvider credentialsProvider) {
        RestClientBuilder.HttpClientConfigCallback httpConfig = new RestClientBuilder.HttpClientConfigCallback() {
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        };
        return httpConfig;
    }

    public static void main(String[] args) throws IOException {


        int from = 0;
        int end = 10000;
        String month = "decembre";

        StringBuffer sb = new StringBuffer();
        if (args.length >= 1) {
            from = Integer.parseInt(args[0]);
        }

        if (args.length >= 2) {
            end = Integer.parseInt(args[1]);
        }


        if (args.length >= 3) {
            month = args[2];
        }

        String currentIndex ="";

        if (args.length >= 4) {
            currentIndex = args[4];
        }

        String endIndexUser ="";

        if (args.length >= 5) {
            endIndexUser = args[5];
        }

        String endIndexPassword ="";

        if (args.length >= 5) {
            endIndexUser = args[5];
        }

        String endIndexUrl ="";

        if (args.length >= 5) {
            endIndexUrl = args[5];
        }


        // Manage Elastic credential
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(endIndexUser, endIndexPassword));

        // Initialize High level Elastic Rest builder client
        RestClientBuilder restClientBuilder = RestClient.builder(HttpHost.create(endIndexUrl));
        restClientBuilder.setMaxRetryTimeoutMillis(1000000);

        // set auth
        restClientBuilder.setHttpClientConfigCallback(getHttpConfig(credentialsProvider));
        RestHighLevelClient client = new RestHighLevelClient(restClientBuilder);

        SearchRequest searchRequest = new SearchRequest(currentIndex);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.from(from);
        sourceBuilder.size(end);

        sourceBuilder.query(QueryBuilders.termQuery("month", month));
        searchRequest.source(sourceBuilder);

        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits searchHits  = searchResponse.getHits();
            for (int i =0; i< searchHits.totalHits; i++) {
                SearchHit currentHit = searchHits.getAt(i);

                sb.append("{\"index\": {\"_type\": \"doc\", \"_id\": "+currentHit.getId()+ ", \"_index\": \"" + currentIndex + "\"}}");
                sb.append("\n");
                sb.append(currentHit.getSourceAsString());
                sb.append("\n");
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        BufferedWriter bwr = new BufferedWriter(new FileWriter(new File("migrate-" +month)));

        //write contents of StringBuffer to a file
        bwr.write(sb.toString());

        //flush the stream
        bwr.flush();

        //close the stream
        bwr.close();

        System.out.println("Content of StringBuffer written to File.");

        exit(0);

    }
}
