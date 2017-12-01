/*
 * Copyright 2017 Marco Helmich
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

package org.carbon.grid;

import org.cfg4j.source.ConfigurationSource;
import org.cfg4j.source.classpath.ClasspathConfigurationSource;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;

import static org.junit.Assert.assertTrue;

public class CarbonGridTest {
    @Test
    public void testStartup() throws IOException {
        try (CarbonGrid grid = CarbonGrid.innerStart(getTestConfig())) {
            assertTrue(grid.isUp());
        }
    }

    private ConfigurationSource getTestConfig() {
        return new ClasspathConfigurationSource(
                () -> Paths.get("carbon-grid.yaml")
        );
    }
}
