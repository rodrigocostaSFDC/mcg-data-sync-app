package com.salesforce.mcg.datasync.common;

import org.apache.logging.log4j.util.Strings;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class AppConstants {

    public static class Company {
        public static final java.lang.String TELMEX = "telmex";
        public static final java.lang.String TELNOR = "telnor";
    }

    public static class Database {
        public static final java.lang.String JDBC_CONN_STRING = "jdbc:postgresql://%s:%s/%s?reWriteBatchedInserts=true%s";
        public static final java.lang.String SSLMODE_REQUIRED = "sslmode=require";
    }

    public static class Sftp {

        public static class Mode {
            public static final java.lang.String MODE = "sftp.mode";
            public static final java.lang.String REMOTE = "remote";
            public static final java.lang.String LOCAL = "local";
        }
        public static class StrictHost {
            public static final java.lang.String HOST_KEY_CHECKING = "StrictHostKeyChecking";
            public static final java.lang.String NO = "no";
            public static final java.lang.String YES = "yes";
        }
        public static class PreferredAuth {
            public static final java.lang.String PREFERRED_AUTHENTICATIONS = "PreferredAuthentications";
            public static final java.lang.String REMOTE = "publickey,password,keyboard-interactive";
            public static final java.lang.String LOCAL = "password";
        }
        public static class Channel {
            public static final java.lang.String SFTP = "sftp";
        }

    }

    public static class Timezone {
        public static final String AMERICA_MEXICO_CITY = "America/Mexico_City";
    }

    public static class Formats {
        public static final String  MILLIS_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    }

    public static class File {
        public static final Charset UTF_8 = StandardCharsets.UTF_8;
        public static class Extensions {
            public static final String ZIP = ".zip";
            public static final String CSV = ".csv";
            public static final String TXT = ".txt";
            public static final String GZ = ".gz";
            public static final String DAT = ".dat";
        }
    }

    public static final String FILENAME_UNSUPPORTED_CHARS_REGEX = "[^a-zA-Z0-9_-]";

    public static final String FILENAME_DATETIME_FORMAT_PATTERN = "yyyyMMdd_HHmm";
    public static final String FILENAME_ERRORS_SUFFIX = ".hasErrors";

    public static final String PHONE_HEADER = "CELULAR";
    public static final String PHONE_HEADER_FALLBACK = "TELEFONO";
    public static final List<String> URL_HEADERS = List.of("URL", "URL2");
    public static final List<String> API_KEY_HEADERS = List.of("TCODE");
    public static final List<String> TEMPLATE_NAME_HEADERS = List.of("TNAME");
    public static final List<String> SUBSCRIBER_KEY_HEADERS = List.of(
            "SUBSCRIBER_KEY",
            "MOBILE_USER_ID");

    public static final String ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    public static final int ZERO = 0;

    static class MessageType {
        public static final java.lang.String SMS = "S";
        public static final java.lang.String EMAIL = "E";
        public static final java.lang.String WHATSAPP = "W";
    }

    public static class Chars {
        public static final char SINGLE_QUOTE = '\'';
        public static final char DOUBLE_QUOTE = '\"';
        public static final char SPACE = ' ';
        public static final char PIPE = '|';
        public static final char UNDERSCORE = '_';
        public static final char DOT = '.';
        public static final char COMMA = ',';
        public static final char FORWARD_SLASH = '/';
        public static final char BACKSLASH = '\\';
        public static final char CR = '\r';
    }

    public static class Strings {
        public static final java.lang.String SINGLE_QUOTE = "'";
        public static final java.lang.String DOUBLE_QUOTE = "\"";
        public static final java.lang.String EMPTY = Strings.EMPTY;
        public static final java.lang.String SPACE = "\\s";
        public static final java.lang.String PIPE = "|";
        public static final java.lang.String UNDERSCORE = "_";
        public static final java.lang.String DOT = ".";
        public static final java.lang.String COMMA = ",";
        public static final java.lang.String FORWARD_SLASH = "/";
        public static final java.lang.String BACKSLASH = "\\";
        public static final java.lang.String CR = "\r";
        public static final java.lang.String NEW_LINE = "\n";

    }
}
