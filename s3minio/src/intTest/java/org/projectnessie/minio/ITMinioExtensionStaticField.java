/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.minio;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith({MinioExtension.class, SoftAssertionsExtension.class})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ITMinioExtensionStaticField {
  @InjectSoftAssertions private SoftAssertions soft;

  @Minio private static MinioAccess minioStaticField;

  static MinioAccess memoizedStaticInstance;

  @Order(101)
  @Test
  public void fields1() {
    soft.assertThat(minioStaticField).isNotNull();

    soft.assertThat(minioStaticField.bucket()).isNotEmpty();
    soft.assertThat(minioStaticField.accessKey()).isNotEmpty();
    soft.assertThat(minioStaticField.secretKey()).isNotEmpty();

    memoizedStaticInstance = minioStaticField;
  }

  @Order(102)
  @Test
  public void fields2() {
    soft.assertThat(minioStaticField).isSameAs(memoizedStaticInstance);
  }
}
