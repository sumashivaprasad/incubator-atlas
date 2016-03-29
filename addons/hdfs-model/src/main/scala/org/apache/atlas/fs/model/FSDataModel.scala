/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.fs.model

import org.apache.atlas.AtlasClient
import org.apache.atlas.typesystem.TypesDef
import org.apache.atlas.typesystem.builders.TypesBuilder
import org.apache.atlas.typesystem.json.TypesSerialization
import org.apache.hadoop.fs.permission.FsAction

/**
 * This represents the data model for a HDFS Path
 */
object FSDataModel extends App {

    var typesDef : TypesDef = null

    val typesBuilder = new TypesBuilder

    import typesBuilder._

    typesDef = types {

        // FS DataSet
        _class(FSDataTypes.FS_PATH.toString, List("DataSet", AtlasClient.REFERENCEABLE_SUPER_TYPE)) {
            // fully qualified path/URI to file or dir is stored in 'qualifiedName'. Hence not having a separate attribute to specify path. This is to keep search consistent
            "path" ~ (string, required, indexed)
            "createTime" ~ (date, required, indexed)
            "modifiedTime" ~ (date, required, indexed)
            //Is a regular file or a directory. If true, it is a file else a directory
            "isFile" ~ (boolean, optional, indexed)
            //Is a symlink or not
            "isSymlink" ~ (boolean, optional, indexed)
            //Is is a relative or absolute path
            "isRelative" ~ (boolean, optional, indexed)
            //Optional and may not be set for a directory
            "fileSize" ~ (int, optional, indexed)
            "owner" ~ (string, optional, indexed)
            "group" ~ (string, optional, indexed)
            "posixPermissions" ~ (FSDataTypes.FS_PERMISSIONS.toString, optional, indexed)
        }

        enum(FSDataTypes.FS_ACTION.toString,  FsAction.values().map(x => x.name()) : _*)

        struct(FSDataTypes.FS_PERMISSIONS.toString) {
            PosixPermissions.PERM_USER.toString ~ (FSDataTypes.FS_ACTION.toString, required, indexed)
            PosixPermissions.PERM_GROUP.toString ~ (FSDataTypes.FS_ACTION.toString, required, indexed)
            PosixPermissions.PERM_OTHER.toString ~ (FSDataTypes.FS_ACTION.toString, required, indexed)
            PosixPermissions.STICKY_BIT.toString ~ (boolean, required, indexed)
            //TODO - ACL
            //TODO - encryption related - ?
        }

        //HDFS DataSet
        _class(FSDataTypes.HDFS_PATH.toString, List(FSDataTypes.FS_PATH.toString)) {
            //Making cluster optional since path is already unique containing the namenode URI
            "cluster" ~ (string, required, indexed)
            "numberOfReplicas" ~ (int, optional, indexed)
        }
    }

    // add the types to atlas
    val typesAsJSON = TypesSerialization.toJson(typesDef)
    println("FS Data Model as JSON: ")
    println(typesAsJSON)

}

object FSDataTypes extends Enumeration {
    type FSDataTypes = Value
    val FS_ACTION, FS_PATH, HDFS_PATH, FS_PERMISSIONS = Value
}

object PosixPermissions extends Enumeration {
    type PosixPermissions = Value
    final val PERM_USER, PERM_GROUP, PERM_OTHER, STICKY_BIT = Value
}
