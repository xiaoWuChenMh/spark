/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.shuffle;

import java.io.File;

import org.apache.spark.network.util.JavaUtils;

public class ExecutorDiskUtils {

  /**
   * Hashes a filename into the corresponding local directory, in a manner consistent with
   * Spark's DiskBlockManager.getFile().
   */
  public static String getFilePath(String[] localDirs, int subDirsPerLocalDir, String filename) {
    // 计算文件名的非负哈希值:filename就是 Shuffle 数据块对应的文件名（由blockId生成），比如shuffle_1_2_3.data；nonNegativeHash保证哈希值是非负数
    int hash = JavaUtils.nonNegativeHash(filename);
    // 选择要使用的主目录: 用哈希值对localDirs的长度取模，结果作为数组下标，从多个目录中选唯一一个主目录；
    String localDir = localDirs[hash % localDirs.length];
    // 选择主目录下的子目录:先把哈希值除以localDirs长度（消去主目录选择的影响），再对subDirsPerLocalDir取模，得到子目录 ID
    //  - subDirsPerLocalDir是每个主目录下的子目录数量（Spark 默认 64 个，对应配置spark.diskStore.subDirectories）；
    //  - 例：哈希值 = 100，localDirs.length=3，subDirsPerLocalDir=64，则(100/3)=33，33%64=33，子目录 ID=33（格式化为21，因为%02x是 16 进制，33 的 16 进制是 21）。
    int subDirId = (hash / localDirs.length) % subDirsPerLocalDir;
    // 拼接并归一化路径: 最终路径格式：主目录/两位16进制子目录ID/文件名；
    final String notNormalizedPath =
      localDir + File.separator + String.format("%02x", subDirId) + File.separator + filename;
    // Interning the normalized path as according to measurements, in some scenarios such
    // duplicate strings may waste a lot of memory (~ 10% of the heap).
    // Unfortunately, we cannot just call the normalization code that java.io.File
    // uses, since it is in the package-private class java.io.FileSystem.
    // So we are creating a File just to get the normalized path back to intern it.
    // We return this interned normalized path.
    // 最后通过new File(notNormalizedPath).getPath().intern()归一化路径（处理分隔符、相对路径等），并驻留字符串（intern）节省内存（避免重复字符串占用堆内存）。
    return new File(notNormalizedPath).getPath().intern();
  }

}
