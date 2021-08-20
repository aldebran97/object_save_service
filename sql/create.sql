CREATE TABLE `oss`
(
    `md5`     bigint DEFAULT NULL,
    `id`      varchar(100) NOT NULL,
    `content` blob,
    `qId`     bigint auto_increment,
    PRIMARY KEY (`id`),
    KEY       `md5` (`md5`),
    UNIQUE KEY `qId` (`qId`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;