package com.accenture.trac.svc.meta.logic;

import java.util.regex.Pattern;


public class MetadataConstants {

    public static final boolean TRUSTED_API = true;
    public static final boolean PUBLIC_API = false;

    public static final int OBJECT_FIRST_VERSION = 1;
    public static final int TAG_FIRST_VERSION = 1;

    // Valid identifiers are made up of alpha-numeric characters and the underscore, starting with a letter

    // Use \\A - \\Z to match the whole input
    // ^...$ would allow matches like "my_var\n_gotcha"

    public static final Pattern VALID_IDENTIFIER = Pattern.compile("\\A[a-zA-Z]\\w*\\Z");

    // Identifiers starting trac_ are reserved for use by the TRAC platform

    public static final Pattern TRAC_RESERVED_IDENTIFIER = Pattern.compile("\\Atrac_.*", Pattern.CASE_INSENSITIVE);
}
