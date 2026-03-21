package com.salesforce.mcg.datasync.common;

import org.springframework.boot.Banner;
import org.springframework.boot.ansi.AnsiOutput;
import org.springframework.core.env.Environment;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

public class SalesforceBanner implements Banner {

    @Override
    public void printBanner(Environment env, Class<?> sourceClass, PrintStream out) {

        // ensure colors are on (can also be done via application.properties)
        AnsiOutput.setEnabled(AnsiOutput.Enabled.ALWAYS);
        AnsiOutput.setConsoleAvailable(true);

        // dynamic bits
        String appName   = env.getProperty("spring.application.name", "application");
        String profiles  = String.join(",", List.of(env.getActiveProfiles()));

        String version = (sourceClass != null && sourceClass.getPackage() != null)
                ? sourceClass.getPackage().getImplementationVersion()
                : null;
        if (version == null) version = "DEV"; // fallback for IDE runs

        String build = "Undefined";
        try (var in = getClass().getResourceAsStream("/META-INF/build-info.properties")) {
            if (in != null) {
                var p = new java.util.Properties();
                p.load(in);
                // keys look like: build.time, build.version, build.name, build.group, build.artifact
                build = p.getProperty("build.time");
            }
        } catch (IOException e) {
            //Nothind to be done;
        }

        if (profiles.isEmpty()) profiles = "(default)";

        String RESET   = "\u001B[0m";
        String FG_BLUE = "\u001B[38;5;33m";

        String banner = RESET + "\n" +
                "                   " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                "                " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "           " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▓" + RESET + "\n" +
                "             " + FG_BLUE + "▓▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "     " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                "            " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + " " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                "           " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                "         " + FG_BLUE + "▓▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + " " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                "         " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                "        " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                "        " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                "       " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                "       " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▓" + RESET + "\n" +
                "       " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                "        "+ FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                "        "+ FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒░░░▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                "        "+ FG_BLUE + "▓▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒ ░▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒ ░▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                "      "+ FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒░░░░▒▒▒▒▒░░░▒▒▒▒ ░▒▒▒▒░░░▒▒▒▒▒░░░░▒▒▒▒░ ░▒▒▒▒░░░▒▒▒▒▒▒▒▒▒▒▒▒▒░░░▒▒▒▒░░░▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                "    "+ FG_BLUE + "▓▒▒▒▒▒▒▒▒▒▒▒ ░▒▒▒▒▒▒▒▒▒▒░ ░▒▒ ░▒▒ ░▒▒▒ ░▒▒ ░▒▒▒▒▒▒▒ ░▒▒▒  ▒▒▒░ ▒▒░ ░▒▒▒▒ ░▒▒▒▒▒░ ▒▒▒░░▒▒▒▒▒▒▒▒▒▒▓" + RESET + "\n" +
                "   "+ FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒░ ░▒▒▒▒▒▒░░░░ ░▒▒ ░▒░ ░░░░░ ▒▒░ ░▒▒▒▒▒▒ ░▒▒▒ ▒▒▒▒▒ ░▒░ ▒▒▒▒░ ▒▒▒▒▒▒ ░░░░░ ▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                "  "+ FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒░ ▒▒ ░▒▒▒ ░▒▒ ░▒░ ░▒▒▒▒▒▒▒▒▒▒▒░░▒▒░ ▒▒▒▒ ▒▒▒▒▒ ░▒░ ▒▒▒▒░ ▒▒▒▒▒▒ ░▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                " " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒░▒▒▒░ ▒▒ ░▒▒▒ ░▒▒ ░▒▒░ ░▒▒░▒▒▒░▒▒▒░ ▒▒░░▒▒▒▒░ ░▒░░░▒▒░ ▒▒▒▒▒░░▒▒▒▒▒░ ░▒▒░░▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                " " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒ ░▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                " " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒░░▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                " " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                " " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▓" + RESET + "\n" +
                " " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                " " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▓" + RESET + "\n" +
                "  " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                "   " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                "    " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                "     " + FG_BLUE + "▓▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                "      " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                "       " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                "        " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                "         " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                "          " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                "            " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                "              " + FG_BLUE + "▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒" + RESET + "\n" +
                "                " + FG_BLUE +"▒▓▒▒▒▒▒▒▒▒▒▒▒▒▒▒\n"+
                " " + RESET + "\n" +
                " Application: " + appName + "\n" +
                " Version: " + version + "\n" +
                " Active profiles: " + profiles + "\n" +
                " Build: " + build + "\n\n\n";

        out.print(banner);

    }

}

