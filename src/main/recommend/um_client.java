import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.examples.recommend.*;

import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class um_client {
    private static final Logger logger = Logger.getLogger(um_client.class.getName());

    private final ManagedChannel channel;
    private final RecommendGrpc.RecommendBlockingStub blockingStub;

    /** Construct client connecting to HelloWorld server at {@code host:port}. */
    public um_client(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext()
                .build());
    }

    /** Construct client for accessing HelloWorld server using the existing channel. */
    um_client(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = RecommendGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /** Say hello to server. */
    public void greet(int id) {
        logger.info("Will try to input userId " + id + " ...");
        UserRequest request = UserRequest.newBuilder().setUserId(id).build();
        MovieResponse response;
        try {
            response = blockingStub.movieRecommend(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        for(int i=0;i< response.getMovieCount();i++) {
            logger.info("Greeting: " + response.getMovie(i));
        }
    }

    /**
     * Greet server. If provided, the first element of {@code args} is the name to use in the
     * greeting.
     */
    public static void main(String[] args) throws Exception {
        um_client client = new um_client("localhost", 50051);
        try {
            /* Access a service running on the local machine on port 50051 */
            while(true) {
                Scanner input = new Scanner(System.in);
                int userId = input.nextInt();
                client.greet(userId);
            }
        } finally {
            client.shutdown();
        }
    }
}
