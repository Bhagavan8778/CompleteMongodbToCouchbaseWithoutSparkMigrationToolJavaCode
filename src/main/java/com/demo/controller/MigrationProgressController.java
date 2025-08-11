//package com.demo.controller;
//
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.messaging.simp.SimpMessagingTemplate;
//import org.springframework.stereotype.Controller;
//
//import com.demo.dto.MigrationProgress;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//@Controller
//public class MigrationProgressController {
//	  private static final Logger logger = LoggerFactory.getLogger(MigrationProgressController.class);
//    @Autowired
//    private SimpMessagingTemplate messagingTemplate;
//
//    public void sendProgressUpdate(String database, String collection, 
//                                  int transferred, int total, String status) {
//        try {
//    	MigrationProgress progress = new MigrationProgress(
//            database, 
//            collection, 
//            transferred, 
//            total, 
//            status
//        );
//        
//        messagingTemplate.convertAndSend(
//            "/topic/migration-progress", 
//            progress
//        );
//        }catch (Exception e) {
//            logger.error("Failed to send progress update", e);
//        }
//    }
//    }

package com.demo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;

import com.demo.dto.MigrationProgress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Controller
public class MigrationProgressController {
    private static final Logger logger = LoggerFactory.getLogger(MigrationProgressController.class);

    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    public void sendProgressUpdate(String database, String collection, 
                                  int transferred, int total, String status) {
        try {
            MigrationProgress progress = new MigrationProgress(
                database, 
                collection, 
                transferred, 
                total, 
                status
            );
        
            messagingTemplate.convertAndSend(
                "/topic/migration-progress", 
                progress
            );
        } catch (Exception e) {
            logger.error("Failed to send progress update", e);
        }
    }
}


