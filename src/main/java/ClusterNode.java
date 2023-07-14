import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;

public class ClusterNode {
    private static final int PRIMARY_NODE_PORT = 12345;
    private static final int SECONDARY_NODE_PORT = 54321;
    private static final String PRIMARY_NODE_HOST = "localhost";

    private ServerSocketChannel primaryServerSocketChannel;
    private ServerSocketChannel secondaryServerSocketChannel;
    private Selector selector;
    private ByteBuffer readBuffer;
    private ByteBuffer writeBuffer;

    private boolean isPrimaryConnected;

    public void startPrimaryNode() {
        try {
            initializePrimaryServerSocket();

            readBuffer = ByteBuffer.allocate(1024);
            writeBuffer = ByteBuffer.allocate(1024);

            System.out.println("Primary node listening on port " + PRIMARY_NODE_PORT);

            while (true) {
                int selectResult = selector.select();
                if (selectResult == 0) {
                    continue;
                }

                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    keyIterator.remove();

                    if (!key.isValid()) {
                        continue;
                    }

                    if (key.isAcceptable()) {
                        acceptSecondaryConnection(key);
                    } else if (key.isReadable()) {
                        if (key.attachment() == null) {
                            readConfirmationFromSecondary(key);
                        } else {
                            readDataFromSecondary(key);
                        }
                    } else if (key.isWritable()) {
                        sendConfirmationToSecondary(key);
                    }
                }
            }
        } catch (IOException e) {
            handlePrimaryNodeFailure(e);
        } finally {
            try {
                selector.close();
                primaryServerSocketChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void initializePrimaryServerSocket() throws IOException {
        if (primaryServerSocketChannel != null) {
            primaryServerSocketChannel.close();
        }

        primaryServerSocketChannel = ServerSocketChannel.open();
        primaryServerSocketChannel.bind(new InetSocketAddress(PRIMARY_NODE_PORT));
        primaryServerSocketChannel.configureBlocking(false);

        if (selector != null) {
            selector.close();
        }

        selector = Selector.open();
        primaryServerSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    private void acceptSecondaryConnection(SelectionKey key) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel socketChannel = serverChannel.accept();
        socketChannel.configureBlocking(false);
        socketChannel.register(selector, SelectionKey.OP_READ);

        System.out.println("Secondary node connected: " + socketChannel.getRemoteAddress());
    }

    private void readDataFromSecondary(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        readBuffer.clear();

        int bytesRead = socketChannel.read(readBuffer);
        if (bytesRead == -1) {
            // Connection closed by the secondary node
            socketChannel.close();
            System.out.println("Secondary node disconnected: " + socketChannel.getRemoteAddress());
            return;
        }

        readBuffer.flip();
        byte[] data = new byte[readBuffer.remaining()];
        readBuffer.get(data);

        // Process the received data from the secondary node
        // Add your logic here

        // Example: Print the received data
        System.out.println("Received from secondary node: " + new String(data));
    }

    private void readConfirmationFromSecondary(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        readBuffer.clear();

        int bytesRead = socketChannel.read(readBuffer);
        if (bytesRead == -1) {
            // Connection closed by the primary node
            socketChannel.close();
            System.out.println("Primary node disconnected: " + socketChannel.getRemoteAddress());
            isPrimaryConnected = false;
            return;
        }

        readBuffer.flip();
        byte[] data = new byte[readBuffer.remaining()];
        readBuffer.get(data);

        String confirmation = new String(data);
        if (confirmation.equals("Primary node is healthy.")) {
            isPrimaryConnected = true;
            System.out.println("Received confirmation from primary node.");
        }
    }

    private void sendConfirmationToSecondary(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();

        String confirmationMessage = "Primary node is healthy.";
        writeBuffer.clear();
        writeBuffer.put(confirmationMessage.getBytes());
        writeBuffer.flip();

        socketChannel.write(writeBuffer);
    }

    private void handlePrimaryNodeFailure(Exception e) {
        e.printStackTrace();

        // Perform any necessary cleanup or reinitialization logic here

        // Example: Reinitialize the server socket
        try {
            primaryServerSocketChannel.close();
            primaryServerSocketChannel = ServerSocketChannel.open();
            primaryServerSocketChannel.bind(new InetSocketAddress(PRIMARY_NODE_PORT));
            primaryServerSocketChannel.configureBlocking(false);
            primaryServerSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void startSecondaryNode() {
        try {
            initializeSecondaryServerSocket();

            readBuffer = ByteBuffer.allocate(1024);
            writeBuffer = ByteBuffer.allocate(1024);

            System.out.println("Secondary node listening on port " + SECONDARY_NODE_PORT);

            while (true) {
                int selectResult = selector.select();
                if (selectResult == 0) {
                    continue;
                }

                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    keyIterator.remove();

                    if (!key.isValid()) {
                        continue;
                    }

                    if (key.isConnectable()) {
                        connectToPrimaryNode(key);
                    } else if (key.isReadable()) {
                        readDataFromPrimaryNode(key);
                    } else if (key.isWritable()) {
                        sendHealthCheckToPrimaryNode(key);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                selector.close();
                secondaryServerSocketChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void initializeSecondaryServerSocket() throws IOException {
        if (secondaryServerSocketChannel != null) {
            secondaryServerSocketChannel.close();
        }

        secondaryServerSocketChannel = ServerSocketChannel.open();
        secondaryServerSocketChannel.bind(new InetSocketAddress(SECONDARY_NODE_PORT));
        secondaryServerSocketChannel.configureBlocking(false);

        if (selector != null) {
            selector.close();
        }

        selector = Selector.open();
        secondaryServerSocketChannel.register(selector, SelectionKey.OP_CONNECT);
    }

    private void connectToPrimaryNode(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        if (socketChannel.isConnectionPending()) {
            socketChannel.finishConnect();
        }

        socketChannel.register(selector, SelectionKey.OP_WRITE);
    }

    private void readDataFromPrimaryNode(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        readBuffer.clear();

        int bytesRead = socketChannel.read(readBuffer);
        if (bytesRead == -1) {
            // Connection closed by the primary node
            socketChannel.close();
            System.out.println("Primary node disconnected: " + socketChannel.getRemoteAddress());
            return;
        }

        readBuffer.flip();
        byte[] data = new byte[readBuffer.remaining()];
        readBuffer.get(data);

        // Process the received data from the primary node
        // Add your logic here

        // Example: Print the received data
        System.out.println("Received from primary node: " + new String(data));
    }

    private void sendHealthCheckToPrimaryNode(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();

        String healthCheckMessage = "Are you alive?";
        writeBuffer.clear();
        writeBuffer.put(healthCheckMessage.getBytes());
        writeBuffer.flip();

        socketChannel.write(writeBuffer);
    }

    public static void main(String[] args) {
        ClusterNode clusterNode = new ClusterNode();
        clusterNode.startPrimaryNode();

        // Uncomment the line below to start the secondary node
        // clusterNode.startSecondaryNode();
    }
}
