**DLP- Data Loss Prevention**

There are multiple types of de-identification techniques shown in below link
https://cloud.google.com/dlp/docs/transformations-reference
1. Redaction: Deletes all or part of a detected sensitive value.
2. Replacement: Replaces a detected sensitive value with a specified surrogate value.
3. Masking: Replaces a number of characters of a sensitive value with a specified surrogate character, such as a hash (#) or asterisk (*).
4. Crypto-based tokenization: Encrypts the original sensitive data value using a cryptographic key. Sensitive Data Protection supports several types of tokenization, including transformations that can be reversed, or "re-identified."

  This is divided in three sub-part
  
    i. Cryptographic hashing https://cloud.google.com/dlp/docs/transformations-reference#crypto-hashing
    
    ii. Format preserving encryption https://cloud.google.com/dlp/docs/transformations-reference#fpe
    
    iii. Deterministic encryption https://cloud.google.com/dlp/docs/transformations-reference#de
    
6. Bucketing: "Generalizes" a sensitive value by replacing it with a range of values. (For example, replacing a specific age with an age range, or temperatures with ranges corresponding to "Hot," "Medium," and "Cold.")
7. Date shifting: Shifts sensitive date values by a random amount of time.
8. Time extraction: Extracts or preserves specified portions of date and time values.

Python Methods and modules: https://cloud.google.com/python/docs/reference/dlp/3.0.1/google.cloud.dlp_v2.types
