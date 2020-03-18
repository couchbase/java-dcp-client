/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.dcp.core.security.sasl;

import com.couchbase.client.dcp.core.utils.Base64;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of a SCRAM-SHA512, SCRAM-SHA256 and SCRAM-SHA1 enabled {@link SaslClient}.
 */
public class ShaSaslClient implements SaslClient {

  private static final byte[] CLIENT_KEY = "Client Key".getBytes();
  private static final byte[] SERVER_KEY = "Server Key".getBytes();

  private final String name;
  private final String hmacAlgorithm;
  private final CallbackHandler callbacks;
  private final MessageDigest digest;

  private String clientNonce;
  private byte[] salt;
  private byte[] saltedPassword;
  private int iterationCount;
  private String clientFirstMessage;
  private String clientFirstMessageBare;
  private String clientFinalMessageNoProof;
  private String serverFirstMessage;
  private String serverFinalMessage;
  private String nonce;

  public ShaSaslClient(CallbackHandler cbh, int sha) throws NoSuchAlgorithmException {
    callbacks = cbh;
    switch (sha) {
      case 512:
        digest = MessageDigest.getInstance("SHA-512");
        name = "SCRAM-SHA512";
        hmacAlgorithm = "HmacSHA512";
        break;
      case 256:
        digest = MessageDigest.getInstance("SHA-256");
        name = "SCRAM-SHA256";
        hmacAlgorithm = "HmacSHA256";
        break;
      case 1:
        digest = MessageDigest.getInstance("SHA-1");
        name = "SCRAM-SHA1";
        hmacAlgorithm = "HmacSHA1";
        break;
      default:
        throw new RuntimeException("Invalid SHA version specified");
    }


    SecureRandom random = new SecureRandom();
    byte[] random_nonce = new byte[21];
    random.nextBytes(random_nonce);
    clientNonce = Base64.encode(random_nonce);
  }

  @Override
  public String getMechanismName() {
    return name;
  }

  @Override
  public boolean hasInitialResponse() {
    return true;
  }

  @Override
  public byte[] evaluateChallenge(byte[] challenge) throws SaslException {
    if (clientFirstMessage == null) {
      if (challenge.length != 0) {
        throw new SaslException("Initial challenge should be without input data");
      }

      clientFirstMessage = "n,,n=" + getUserName() + ",r=" + clientNonce;
      clientFirstMessageBare = clientFirstMessage.substring(3);
      return clientFirstMessage.getBytes();
    } else if (serverFirstMessage == null) {
      serverFirstMessage = new String(challenge);

      HashMap<String, String> attributes = new HashMap<String, String>();
      try {
        decodeAttributes(attributes, serverFirstMessage);
      } catch (Exception ex) {
        throw new SaslException("Could not decode attributes from server message \""
            + serverFirstMessage + "\"", ex);
      }

      for (Map.Entry<String, String> entry : attributes.entrySet()) {
        switch (entry.getKey().charAt(0)) {
          case 'r':
            nonce = entry.getValue();
            break;
          case 's':
            salt = Base64.decode(entry.getValue());
            break;
          case 'i':
            iterationCount = Integer.parseInt(entry.getValue());
            break;
          default:
            throw new IllegalArgumentException("Invalid key supplied in the serverFirstMessage");
        }
      }

      // validate that all of the mandatory parameters was present!!
      if (!attributes.containsKey("r") || !attributes.containsKey("s") || !attributes.containsKey("i")) {
        throw new IllegalArgumentException("missing mandatory key in serverFirstMessage");
      }

      // We have the salt, time to generate the salted password
      generateSaltedPassword();

      clientFinalMessageNoProof = "c=biws,r=" + nonce;
      String client_final_message = clientFinalMessageNoProof + ",p=" + Base64.encode(getClientProof());
      return client_final_message.getBytes();
    } else if (serverFinalMessage == null) {
      serverFinalMessage = new String(challenge);

      HashMap<String, String> attributes = new HashMap<String, String>();
      try {
        decodeAttributes(attributes, serverFinalMessage);
      } catch (Exception ex) {
        throw new SaslException("Could not decode attributes from server message \""
            + serverFinalMessage + "\"", ex);
      }

      if (attributes.containsKey("e")) {
        throw new SaslException("Authentication failure: " + attributes.get("e"));
      }

      if (!attributes.containsKey("v")) {
        throw new SaslException("Syntax error from the server");
      }

      String myServerSignature = Base64.encode(getServerSignature());
      if (!myServerSignature.equals(attributes.get("v"))) {
        throw new SaslException("Server signature is incorrect");
      }

      return new byte[0];
    }

    throw new SaslException("Can't evaluate challenge on a session which is complete");
  }

  @Override
  public boolean isComplete() {
    return serverFinalMessage != null;
  }

  @Override
  public byte[] unwrap(byte[] incoming, int offset, int len) throws SaslException {
    return new byte[0];
  }

  @Override
  public byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException {
    return new byte[0];
  }

  @Override
  public Object getNegotiatedProperty(String propName) {
    return null;
  }

  @Override
  public void dispose() throws SaslException {

  }

  private String getUserName() throws SaslException {
    final NameCallback nameCallback = new NameCallback("Username");
    try {
      callbacks.handle(new Callback[] { nameCallback });
    } catch (IOException e) {
      throw new SaslException("Missing callback fetch username", e);
    } catch (UnsupportedCallbackException e) {
      throw new SaslException("Missing callback fetch username", e);
    }

    final String name = nameCallback.getName();
    if (name == null || name.isEmpty()) {
      throw new SaslException("Missing username");
    }
    return name;
  }

  /**
   * Generate the HMAC with the given SHA algorithm
   */
  private byte[] hmac(byte[] key, byte[] data) {
    try {
      final Mac mac = Mac.getInstance(hmacAlgorithm);
      mac.init(new SecretKeySpec(key, mac.getAlgorithm()));
      return mac.doFinal(data);
    } catch (InvalidKeyException e) {
      if (key.length == 0) {
        throw new UnsupportedOperationException("This JVM does not support empty HMAC keys (empty passwords). "
            + "Please set a bucket password or upgrade your JVM.");
      } else {
        throw new RuntimeException("Failed to generate HMAC hash for password", e);
      }
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  /**
   * Generate the hash of the function.. Unfortunately we couldn't use
   * the one provided by the Java framework because it didn't support
   * others than SHA1. See https://www.ietf.org/rfc/rfc5802.txt (page 6)
   * for how it is generated.
   *
   * @param password   The password to use
   * @param salt       The salt used to salt the hash function
   * @param iterations The number of iterations to sue
   * @return The pbkdf2 version of the password
   */
  private byte[] pbkdf2(final String password, final byte[] salt, int iterations) {
    try {
      Mac mac = Mac.getInstance(hmacAlgorithm);
      Key key;
      if (password == null || password.isEmpty()) {
        key = new EmptySecretKey(hmacAlgorithm);
      } else {
        key = new SecretKeySpec(password.getBytes(), hmacAlgorithm);
      }
      mac.init(key);
      mac.update(salt);
      mac.update("\00\00\00\01".getBytes()); // Append INT(1)

      byte[] un = mac.doFinal();
      mac.update(un);
      byte[] uprev = mac.doFinal();
      xor(un, uprev);

      for (int i = 2; i < iterations; ++i) {
        mac.update(uprev);
        uprev = mac.doFinal();
        xor(un, uprev);
      }

      return un;
    } catch (InvalidKeyException e) {
      if (password == null || password.isEmpty()) {
        throw new UnsupportedOperationException("This JVM does not support empty HMAC keys (empty passwords). "
            + "Please set a bucket password or upgrade your JVM.");
      } else {
        throw new RuntimeException("Failed to generate HMAC hash for password", e);
      }
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  /**
   * XOR the two arrays and store the result in the first one.
   *
   * @param result Where to store the result
   * @param other  The other array to xor with
   */
  private static void xor(byte[] result, byte[] other) {
    for (int i = 0; i < result.length; ++i) {
      result[i] = (byte) (result[i] ^ other[i]);
    }
  }

  private void generateSaltedPassword() throws SaslException {
    final PasswordCallback passwordCallback = new PasswordCallback("Password", false);
    try {
      callbacks.handle(new Callback[]{passwordCallback});
    } catch (IOException e) {
      throw new SaslException("Missing callback fetch password", e);
    } catch (UnsupportedCallbackException e) {
      throw new SaslException("Missing callback fetch password", e);
    }

    final char[] pw = passwordCallback.getPassword();
    if (pw == null) {
      throw new SaslException("Password can't be null");
    }

    String password = new String(pw);
    saltedPassword = pbkdf2(password, salt, iterationCount);
    passwordCallback.clearPassword();
  }

  /**
   * Generate the Server Signature. It is computed as:
   * <p></p>
   * SaltedPassword  := Hi(Normalize(password), salt, i)
   * ServerKey       := HMAC(SaltedPassword, "Server Key")
   * ServerSignature := HMAC(ServerKey, AuthMessage)
   */
  private byte[] getServerSignature() {
    byte[] serverKey = hmac(saltedPassword, SERVER_KEY);
    return hmac(serverKey, getAuthMessage().getBytes());
  }

  /**
   * Generate the Client Proof. It is computed as:
   * <p></p>
   * SaltedPassword  := Hi(Normalize(password), salt, i)
   * ClientKey       := HMAC(SaltedPassword, "Client Key")
   * StoredKey       := H(ClientKey)
   * AuthMessage     := client-first-message-bare + "," +
   * server-first-message + "," +
   * client-final-message-without-proof
   * ClientSignature := HMAC(StoredKey, AuthMessage)
   * ClientProof     := ClientKey XOR ClientSignature
   */
  private byte[] getClientProof() {
    byte[] clientKey = hmac(saltedPassword, CLIENT_KEY);
    byte[] storedKey = digest.digest(clientKey);
    byte[] clientSignature = hmac(storedKey, getAuthMessage().getBytes());

    xor(clientKey, clientSignature);
    return clientKey;
  }

  private static void decodeAttributes(HashMap<String, String> attributes, String string) {
    String[] tokens = string.split(",");
    for (String token : tokens) {
      int idx = token.indexOf('=');
      if (idx != 1) {
        throw new IllegalArgumentException("the input string is not according to the spec");
      }
      String key = token.substring(0, 1);
      if (attributes.containsKey(key)) {
        throw new IllegalArgumentException("The key " + key + " is specified multiple times");
      }
      attributes.put(key, token.substring(2));
    }
  }

  /**
   * Get the AUTH message (as specified in the RFC)
   */
  private String getAuthMessage() {
    if (clientFirstMessageBare == null) {
      throw new RuntimeException("can't call getAuthMessage without clientFirstMessageBare is set");
    }
    if (serverFirstMessage == null) {
      throw new RuntimeException("can't call getAuthMessage without serverFirstMessage is set");
    }
    if (clientFinalMessageNoProof == null) {
      throw new RuntimeException("can't call getAuthMessage without clientFinalMessageNoProof is set");
    }
    return clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageNoProof;
  }

  /**
   * SecretKeySpec doesn't support an empty password, god knows why.
   * so lets just fake it till they make it!
   */
  private static class EmptySecretKey implements SecretKey {
    private final String algorithm;

    public EmptySecretKey(String algorithm) {
      this.algorithm = algorithm;
    }

    @Override
    public String getAlgorithm() {
      return algorithm;
    }

    @Override
    public String getFormat() {
      return "RAW";
    }

    @Override
    public byte[] getEncoded() {
      return new byte[] {};
    }
  }
}
