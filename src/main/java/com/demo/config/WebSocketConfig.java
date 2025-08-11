//package com.demo.config;
//
//import org.springframework.messaging.simp.config.MessageBrokerRegistry;
//import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
//import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
//import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;
//import org.springframework.context.annotation.Configuration;
//
//@Configuration
//@EnableWebSocketMessageBroker
//public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {
//    
////    @Override
////    public void configureMessageBroker(MessageBrokerRegistry config) {
////        config.enableSimpleBroker("/topic", "/queue");
////        config.setApplicationDestinationPrefixes("/app");
////        config.setUserDestinationPrefix("/user");
////    }
//	
//	@Override
//    public void configureMessageBroker(MessageBrokerRegistry registry) {
//        registry.setApplicationDestinationPrefixes("/app"); // 👈 For client -> server messages
//        registry.enableSimpleBroker("/topic");               // 👈 For server -> client messages
//    }
//
//    @Override
//    public void registerStompEndpoints(StompEndpointRegistry registry) {
//        registry.addEndpoint("/ws-migration")
//                .setAllowedOriginPatterns("http://localhost:3000")
//                .withSockJS();
//        
//        registry.addEndpoint("/ws-functions")
//                .setAllowedOriginPatterns("http://localhost:3000")
//                .withSockJS();
//    }
//}


package com.demo.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.*;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.springframework.web.filter.CorsFilter;
import org.springframework.context.annotation.Bean;

@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        registry.setApplicationDestinationPrefixes("/app");
        registry.enableSimpleBroker("/topic", "/queue"); // Added /queue for direct messaging
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        // For SockJS clients
        registry.addEndpoint("/ws-migration", "/ws-functions")
                .setAllowedOriginPatterns("http://localhost:3000", "http://127.0.0.1:3000")
                .withSockJS()
                .setSuppressCors(false); // Changed to false to enable CORS handling

        // For native WebSocket clients (fallback)
        registry.addEndpoint("/ws-migration", "/ws-functions")
                .setAllowedOriginPatterns("http://localhost:3000", "http://127.0.0.1:3000");
    }

    @Bean
    public CorsFilter corsFilter() {
        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        CorsConfiguration config = new CorsConfiguration();
        
        // Configure CORS
        config.setAllowCredentials(true);
        config.addAllowedOrigin("http://localhost:3000");
        config.addAllowedOrigin("http://127.0.0.1:3000"); // Added alternative localhost
        config.addAllowedHeader("*");
        config.addAllowedMethod("*");
        config.addExposedHeader("Authorization"); // Expose auth header if needed
        
        // Important for SockJS
        config.addExposedHeader("Access-Control-Allow-Origin");
        config.addExposedHeader("Access-Control-Allow-Credentials");
        
        source.registerCorsConfiguration("/**", config);
        return new CorsFilter(source);
    }

    // Add transport configuration for better stability
    @Override
    public void configureWebSocketTransport(WebSocketTransportRegistration registration) {
        registration.setMessageSizeLimit(1024 * 1024); // 1MB max message size
        registration.setSendTimeLimit(20 * 10000); // 20 seconds
        registration.setSendBufferSizeLimit(1024 * 1024); // 1MB buffer
    }
}
