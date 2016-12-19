/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.web.params;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Objects;

/**
 * An abstract base class from which to build Jersey parameter classes.
 *
 * @param <T> the type of value wrapped by the parameter
 */
public abstract class AbstractParam<T> {
    private final T value;

    /**
     * Given an input value from a client, creates a parameter wrapping its parsed value.
     *
     * @param input an input value from a client request
     */
    @SuppressWarnings({"AbstractMethodCallInConstructor", "OverriddenMethodCallDuringObjectConstruction"})
    protected AbstractParam(String input) {
        try {
            this.value = parse(input);
        } catch (Exception e) {
            throw new WebApplicationException(error(input, e));
        }
    }

    /**
     * Given a string representation which was unable to be parsed and the exception thrown, produce
     * a {@link javax.ws.rs.core.Response} to be sent to the client.
     *
     * By default, generates a {@code 400 Bad Request} with a plain text entity generated by
     * {@link #errorMessage(String, Exception)}.
     *
     * @param input the raw input value
     * @param e the exception thrown while parsing {@code input}
     * @return the {@link javax.ws.rs.core.Response} to be sent to the client
     */
    protected Response error(String input, Exception e) {
        return Response.status(getErrorStatus()).entity(errorMessage(input, e)).type(mediaType()).build();
    }

    /**
     * Returns the media type of the error message entity.
     *
     * @return the media type of the error message entity
     */
    protected MediaType mediaType() {
        return MediaType.TEXT_PLAIN_TYPE;
    }

    /**
     * Given a string representation which was unable to be parsed and the exception thrown, produce
     * an entity to be sent to the client.
     *
     * @param input the raw input value
     * @param e the exception thrown while parsing {@code input}
     * @return the error message to be sent the client
     */
    protected String errorMessage(String input, Exception e) {
        return String.format("Invalid parameter: %s (%s)", input, e.getMessage());
    }

    /**
     * Given a string representation which was unable to be parsed, produce a {@link javax.ws.rs
     * .core.Response.Status} for the
     * {@link Response} to be sent to the client.
     *
     * @return the HTTP {@link javax.ws.rs.core.Response.Status} of the error message
     */
    @SuppressWarnings("MethodMayBeStatic")
    protected Response.Status getErrorStatus() {
        return Response.Status.BAD_REQUEST;
    }

    /**
     * Given a string representation, parse it and return an instance of the parameter type.
     *
     * @param input the raw input
     * @return {@code input}, parsed as an instance of {@code T}
     * @throws Exception if there is an error parsing the input
     */
    protected abstract T parse(String input) throws Exception;

    /**
     * Returns the underlying value.
     *
     * @return the underlying value
     */
    public T get() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractParam<?> that = (AbstractParam<?>) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return value.toString();
    }
}