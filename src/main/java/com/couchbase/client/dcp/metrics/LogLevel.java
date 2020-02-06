/*
 * Copyright 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.dcp.metrics;

import org.slf4j.Logger;
import org.slf4j.Marker;

public enum LogLevel {
  NONE {
    @Override
    public void log(Logger logger, String message) {
    }

    @Override
    public void log(Logger logger, String message, Throwable t) {
    }

    @Override
    public void log(Logger logger, String format, Object arg) {
    }

    @Override
    public void log(Logger logger, String format, Object arg1, Object arg2) {
    }

    @Override
    public void log(Logger logger, String format, Object... args) {
    }

    @Override
    public boolean isEnabled(Logger logger) {
      return false;
    }

    @Override
    public void log(Marker marker, Logger logger, String message) {
    }

    @Override
    public void log(Marker marker, Logger logger, String message, Throwable t) {
    }

    @Override
    public void log(Marker marker, Logger logger, String format, Object arg) {
    }

    @Override
    public void log(Marker marker, Logger logger, String format, Object arg1, Object arg2) {
    }

    @Override
    public void log(Marker marker, Logger logger, String format, Object... args) {
    }

    @Override
    public boolean isEnabled(Marker marker, Logger logger) {
      return false;
    }
  },

  TRACE {
    @Override
    public void log(Logger logger, String message) {
      logger.trace(message);
    }

    @Override
    public void log(Logger logger, String message, Throwable t) {
      logger.trace(message, t);
    }

    @Override
    public void log(Logger logger, String format, Object arg) {
      logger.trace(format, arg);
    }

    @Override
    public void log(Logger logger, String format, Object arg1, Object arg2) {
      logger.trace(format, arg1, arg2);
    }

    @Override
    public void log(Logger logger, String format, Object... args) {
      logger.trace(format, args);
    }

    @Override
    public boolean isEnabled(Logger logger) {
      return logger.isTraceEnabled();
    }

    @Override
    public void log(Marker marker, Logger logger, String message) {
      logger.trace(marker, message);
    }

    @Override
    public void log(Marker marker, Logger logger, String message, Throwable t) {
      logger.trace(marker, message, t);
    }

    @Override
    public void log(Marker marker, Logger logger, String format, Object arg) {
      logger.trace(marker, format, arg);
    }

    @Override
    public void log(Marker marker, Logger logger, String format, Object arg1, Object arg2) {
      logger.trace(marker, format, arg1, arg2);
    }

    @Override
    public void log(Marker marker, Logger logger, String format, Object... args) {
      logger.trace(marker, format, args);
    }

    @Override
    public boolean isEnabled(Marker marker, Logger logger) {
      return logger.isTraceEnabled(marker);
    }
  },

  DEBUG {
    @Override
    public void log(Logger logger, String message) {
      logger.debug(message);
    }

    @Override
    public void log(Logger logger, String message, Throwable t) {
      logger.debug(message, t);
    }

    @Override
    public void log(Logger logger, String format, Object arg) {
      logger.debug(format, arg);
    }

    @Override
    public void log(Logger logger, String format, Object arg1, Object arg2) {
      logger.debug(format, arg1, arg2);
    }

    @Override
    public void log(Logger logger, String format, Object... args) {
      logger.debug(format, args);
    }

    @Override
    public boolean isEnabled(Logger logger) {
      return logger.isDebugEnabled();
    }

    @Override
    public void log(Marker marker, Logger logger, String message) {
      logger.debug(marker, message);
    }

    @Override
    public void log(Marker marker, Logger logger, String message, Throwable t) {
      logger.debug(marker, message, t);
    }

    @Override
    public void log(Marker marker, Logger logger, String format, Object arg) {
      logger.debug(marker, format, arg);
    }

    @Override
    public void log(Marker marker, Logger logger, String format, Object arg1, Object arg2) {
      logger.debug(marker, format, arg1, arg2);
    }

    @Override
    public void log(Marker marker, Logger logger, String format, Object... args) {
      logger.debug(marker, format, args);
    }

    @Override
    public boolean isEnabled(Marker marker, Logger logger) {
      return logger.isDebugEnabled(marker);
    }
  },

  INFO {
    @Override
    public void log(Logger logger, String message) {
      logger.info(message);
    }

    @Override
    public void log(Logger logger, String message, Throwable t) {
      logger.info(message, t);
    }

    @Override
    public void log(Logger logger, String format, Object arg) {
      logger.info(format, arg);
    }

    @Override
    public void log(Logger logger, String format, Object arg1, Object arg2) {
      logger.info(format, arg1, arg2);
    }

    @Override
    public void log(Logger logger, String format, Object... args) {
      logger.info(format, args);
    }

    @Override
    public boolean isEnabled(Logger logger) {
      return logger.isInfoEnabled();
    }

    @Override
    public void log(Marker marker, Logger logger, String message) {
      logger.info(marker, message);
    }

    @Override
    public void log(Marker marker, Logger logger, String message, Throwable t) {
      logger.info(marker, message, t);
    }

    @Override
    public void log(Marker marker, Logger logger, String format, Object arg) {
      logger.info(marker, format, arg);
    }

    @Override
    public void log(Marker marker, Logger logger, String format, Object arg1, Object arg2) {
      logger.info(marker, format, arg1, arg2);
    }

    @Override
    public void log(Marker marker, Logger logger, String format, Object... args) {
      logger.info(marker, format, args);
    }

    @Override
    public boolean isEnabled(Marker marker, Logger logger) {
      return logger.isInfoEnabled(marker);
    }
  },

  WARN {
    @Override
    public void log(Logger logger, String message) {
      logger.warn(message);
    }

    @Override
    public void log(Logger logger, String message, Throwable t) {
      logger.warn(message, t);
    }

    @Override
    public void log(Logger logger, String format, Object arg) {
      logger.warn(format, arg);
    }

    @Override
    public void log(Logger logger, String format, Object arg1, Object arg2) {
      logger.warn(format, arg1, arg2);
    }

    @Override
    public void log(Logger logger, String format, Object... args) {
      logger.warn(format, args);
    }

    @Override
    public boolean isEnabled(Logger logger) {
      return logger.isWarnEnabled();
    }

    @Override
    public void log(Marker marker, Logger logger, String message) {
      logger.warn(marker, message);
    }

    @Override
    public void log(Marker marker, Logger logger, String message, Throwable t) {
      logger.warn(marker, message, t);
    }

    @Override
    public void log(Marker marker, Logger logger, String format, Object arg) {
      logger.warn(marker, format, arg);
    }

    @Override
    public void log(Marker marker, Logger logger, String format, Object arg1, Object arg2) {
      logger.warn(marker, format, arg1, arg2);
    }

    @Override
    public void log(Marker marker, Logger logger, String format, Object... args) {
      logger.warn(marker, format, args);
    }

    @Override
    public boolean isEnabled(Marker marker, Logger logger) {
      return logger.isWarnEnabled(marker);
    }
  },

  ERROR {
    @Override
    public void log(Logger logger, String message) {
      logger.error(message);
    }

    @Override
    public void log(Logger logger, String message, Throwable t) {
      logger.error(message, t);
    }

    @Override
    public void log(Logger logger, String format, Object arg) {
      logger.error(format, arg);
    }

    @Override
    public void log(Logger logger, String format, Object arg1, Object arg2) {
      logger.error(format, arg1, arg2);
    }

    @Override
    public void log(Logger logger, String format, Object... args) {
      logger.error(format, args);
    }

    @Override
    public boolean isEnabled(Logger logger) {
      return logger.isErrorEnabled();
    }

    @Override
    public void log(Marker marker, Logger logger, String message) {
      logger.error(marker, message);
    }

    @Override
    public void log(Marker marker, Logger logger, String message, Throwable t) {
      logger.error(marker, message, t);
    }

    @Override
    public void log(Marker marker, Logger logger, String format, Object arg) {
      logger.error(marker, format, arg);
    }

    @Override
    public void log(Marker marker, Logger logger, String format, Object arg1, Object arg2) {
      logger.error(marker, format, arg1, arg2);
    }

    @Override
    public void log(Marker marker, Logger logger, String format, Object... args) {
      logger.error(marker, format, args);
    }

    @Override
    public boolean isEnabled(Marker marker, Logger logger) {
      return logger.isErrorEnabled(marker);
    }
  };

  public abstract void log(Logger logger, String message);

  public abstract void log(Logger logger, String message, Throwable t);

  public abstract void log(Logger logger, String format, Object arg);

  public abstract void log(Logger logger, String format, Object arg1, Object arg2);

  public abstract void log(Logger logger, String format, Object... args);

  public abstract boolean isEnabled(Logger logger);

  public abstract void log(Marker marker, Logger logger, String message);

  public abstract void log(Marker marker, Logger logger, String message, Throwable t);

  public abstract void log(Marker marker, Logger logger, String format, Object arg);

  public abstract void log(Marker marker, Logger logger, String format, Object arg1, Object arg2);

  public abstract void log(Marker marker, Logger logger, String format, Object... args);

  public abstract boolean isEnabled(Marker marker, Logger logger);
}
