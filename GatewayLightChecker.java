package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import org.apache.commons.io.FileUtils;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.*;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

public class GatewayLightChecker {

    private static final Logger logger = LoggerFactory.getLogger(GatewayLightChecker.class);

    private static final String TENANT_ID = System.getenv().getOrDefault("TENANT_ID", "your_tenant_id");
    private static final String CLIENT_ID = System.getenv().getOrDefault("CLIENT_ID", "your_client_id");
    private static final String CLIENT_SECRET = System.getenv().getOrDefault("CLIENT_SECRET", "your_client_secret");
    private static final String SCOPE = "https://graph.microsoft.com/.default";
    private static final String TOKEN_URL = "https://login.microsoftonline.com/" + TENANT_ID + "/oauth2/v2.0/token";
    private static final String API_URL = "https://api.example.com/v1/completions";
    private static final File TOKEN_FILE = new File("token_info.json");

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        try {
            List<String> imagePaths = List.of("gateway_image1.jpg", "gateway_image2.jpg", "gateway_image3.jpg");
            String token = getAccessToken();
            String response = callApi(token, imagePaths);
            logger.info("API Response: {}", response);
        } catch (Exception e) {
            logger.error("Error during execution", e);
        }
        long end = System.currentTimeMillis();
        logger.info("Total execution time: {} seconds", (end - start) / 1000.0);
    }

    private static String getAccessToken() throws IOException {
        long currentTime = System.currentTimeMillis() / 1000;

        if (TOKEN_FILE.exists()) {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(TOKEN_FILE);
            if (node.has("expires_at") && currentTime < node.get("expires_at").asLong()) {
                logger.info("Reusing existing token.");
                return node.get("access_token").asText();
            }
        }

        logger.info("Generating new token.");
        String form = "client_id=" + CLIENT_ID +
                      "&scope=" + SCOPE +
                      "&client_secret=" + CLIENT_SECRET +
                      "&grant_type=client_credentials";

        HttpPost post = new HttpPost(TOKEN_URL);
        post.setEntity(new StringEntity(form, ContentType.APPLICATION_FORM_URLENCODED));

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            try (CloseableHttpResponse response = client.execute(post)) {
                int statusCode = response.getCode();
                if (statusCode != 200) throw new IOException("Failed to get token");

                ObjectMapper mapper = new ObjectMapper();
                JsonNode json = mapper.readTree(response.getEntity().getContent());
                String accessToken = json.get("access_token").asText();

                ObjectNode tokenInfo = mapper.createObjectNode();
                tokenInfo.put("access_token", accessToken);
                tokenInfo.put("expires_at", currentTime + 3600);

                mapper.writeValue(TOKEN_FILE, tokenInfo);
                return accessToken;
            }
        }
    }

    private static String encodeImage(String path) throws IOException {
        byte[] imageBytes = FileUtils.readFileToByteArray(new File(path));
        return Base64.getEncoder().encodeToString(imageBytes);
    }

    private static String callApi(String token, List<String> imagePaths) throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        ArrayNode imagesBase64 = mapper.createArrayNode();
        for (String path : imagePaths) {
            ObjectNode imageNode = mapper.createObjectNode();
            imageNode.put("image", encodeImage(path));
            imagesBase64.add(imageNode);
        }
        imagesBase64.addObject().put("type", "text").put("text",
            "Which lights are ON in these images of the AT&T Residential Gateway? Are any lights OFF?");

        ArrayNode messages = mapper.createArrayNode();
        messages.addObject()
            .put("role", "system")
            .put("content", "You are an AI assistant trained to detect the status of lights on an AT&T Residential Gateway (RG). The lights should be identified as ON or OFF based on the visual data. If possible, identify which specific lights are ON.");
        ObjectNode userNode = mapper.createObjectNode();
        userNode.put("role", "user");
        userNode.set("content", imagesBase64);
        messages.add(userNode);

        ObjectNode modelPayload = mapper.createObjectNode();
        modelPayload.set("messages", messages);
        modelPayload.put("temperature", 0.5);
        modelPayload.put("top_p", 0.95);
        modelPayload.put("max_tokens", 3000);

        ObjectNode requestBody = mapper.createObjectNode();
        requestBody.put("domainName", "GenerativeAI");
        requestBody.put("modelName", "gpt-4o");
        requestBody.set("modelPayload", modelPayload);

        StringEntity requestEntity = new StringEntity(requestBody.toString(), ContentType.APPLICATION_JSON);
        HttpPost post = new HttpPost(API_URL);
        post.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token);
        post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        post.setEntity(requestEntity);

        long start = System.currentTimeMillis();
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            try (CloseableHttpResponse response = client.execute(post)) {
                long end = System.currentTimeMillis();
                logger.info("API call completed in {} seconds", (end - start) / 1000.0);

                if (response.getCode() != 200) {
                    throw new IOException("API call failed with status " + response.getCode());
                }

                return new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
            }
        }
    }
}
