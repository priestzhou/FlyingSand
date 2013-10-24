(ns utilities.aes
    (:use
        [utilities.core :only (str->bytes bytes->str)]
    )
    (:import
        [javax.crypto Cipher KeyGenerator SecretKey]
        [javax.crypto.spec SecretKeySpec]
        [java.security SecureRandom]
        [org.apache.commons.codec.binary Base64]
    )
)

(defn- base64 [b]
  (Base64/encodeBase64String b))

(defn- debase64 [s]
  (Base64/decodeBase64 (str->bytes s)))

(defn- get-raw-key [seed]
  (let [keygen (KeyGenerator/getInstance "AES")
        sr (SecureRandom/getInstance "SHA1PRNG")]
    (.setSeed sr (str->bytes seed))
    (.init keygen 128 sr)
    (.. keygen generateKey getEncoded)))

(defn- get-cipher [mode seed]
  (let [key-spec (SecretKeySpec. (get-raw-key seed) "AES")
        cipher (Cipher/getInstance "AES")]
    (.init cipher mode key-spec)
    cipher))

(defn encrypt [text key]
  (let [bytes (str->bytes text)
        cipher (get-cipher Cipher/ENCRYPT_MODE key)]
    (base64 (.doFinal cipher bytes))))

(defn decrypt [text key]
  (let [cipher (get-cipher Cipher/DECRYPT_MODE key)]
    (bytes->str (.doFinal cipher (debase64 text)))))

