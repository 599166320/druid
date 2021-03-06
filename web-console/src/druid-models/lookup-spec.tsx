/*
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

import { Code } from '@blueprintjs/core';
import React from 'react';

import { AutoForm, Field } from '../components';
import { deepGet, deepSet, oneOf, typeIs } from '../utils';

export interface ExtractionNamespaceSpec {
  readonly type: string;
  readonly uri?: string;
  readonly uriPrefix?: string;
  readonly fileRegex?: string;
  readonly namespaceParseSpec?: NamespaceParseSpec;
  readonly connectorConfig?: {
    readonly createTables: boolean;
    readonly connectURI: string;
    readonly user: string;
    readonly password: string;
  };
  readonly table?: string;
  readonly keyColumn?: string;
  readonly valueColumn?: string;
  readonly filter?: any;
  readonly tsColumn?: string;
  readonly pollPeriod?: number | string;
}

export interface NamespaceParseSpec {
  readonly format: string;
  readonly columns?: string[];
  readonly keyColumn?: string;
  readonly valueColumn?: string;
  readonly hasHeaderRow?: boolean;
  readonly skipHeaderRows?: number;
  readonly keyFieldName?: string;
  readonly valueFieldName?: string;
  readonly delimiter?: string;
  readonly listDelimiter?: string;
}

export interface LookupSpec {
  readonly type: string;
  readonly map?: Record<string, string | number>;
  readonly extractionNamespace?: ExtractionNamespaceSpec;
  readonly firstCacheTimeout?: number;
  readonly injective?: boolean;
}

export const LOOKUP_FIELDS: Field<LookupSpec>[] = [
  {
    name: 'type',
    type: 'string',
    suggestions: ['map', 'cachedNamespace'],
    required: true,
    adjustment: l => {
      if (l.type === 'map' && !l.map) {
        return deepSet(l, 'map', {});
      }
      if (l.type === 'cachedNamespace' && !deepGet(l, 'extractionNamespace.type')) {
        return deepSet(l, 'extractionNamespace', { type: 'uri' });
      }
      return l;
    },
  },

  // map lookups are simple
  {
    name: 'map',
    type: 'json',
    height: '60vh',
    defined: typeIs('map'),
    required: true,
    issueWithValue: value => {
      if (!value) return 'map must be defined';
      if (typeof value !== 'object') return `map must be an object`;
      for (const k in value) {
        const typeValue = typeof value[k];
        if (typeValue !== 'string' && typeValue !== 'number') {
          return `map key '${k}' is of the wrong type '${typeValue}'`;
        }
      }
      return;
    },
  },

  // cachedNamespace lookups have more options
  {
    name: 'extractionNamespace.type',
    label: 'Globally cached lookup type',
    type: 'string',
    placeholder: 'uri',
    suggestions: ['uri', 'jdbc'],
    defined: typeIs('cachedNamespace'),
    required: true,
  },
  {
    name: 'extractionNamespace.uriPrefix',
    label: 'URI prefix',
    type: 'string',
    placeholder: 's3://bucket/some/key/prefix/',
    defined: l =>
      deepGet(l, 'extractionNamespace.type') === 'uri' && !deepGet(l, 'extractionNamespace.uri'),
    required: l =>
      !deepGet(l, 'extractionNamespace.uriPrefix') && !deepGet(l, 'extractionNamespace.uri'),
    info:
      'A URI which specifies a directory (or other searchable resource) in which to search for files',
  },
  {
    name: 'extractionNamespace.uri',
    type: 'string',
    label: 'URI (deprecated)',
    placeholder: 's3://bucket/some/key/prefix/lookups-01.gz',
    defined: l =>
      deepGet(l, 'extractionNamespace.type') === 'uri' &&
      !deepGet(l, 'extractionNamespace.uriPrefix'),
    required: l =>
      !deepGet(l, 'extractionNamespace.uriPrefix') && !deepGet(l, 'extractionNamespace.uri'),
    info: (
      <>
        <p>URI for the file of interest, specified as a file, hdfs, or s3 path</p>
        <p>The URI prefix option is strictly better than URI and should be used instead</p>
      </>
    ),
  },
  {
    name: 'extractionNamespace.fileRegex',
    label: 'File regex',
    type: 'string',
    defaultValue: '.*',
    defined: l =>
      deepGet(l, 'extractionNamespace.type') === 'uri' &&
      Boolean(deepGet(l, 'extractionNamespace.uriPrefix')),
    info: 'Optional regex for matching the file name under uriPrefix.',
  },

  // namespaceParseSpec
  {
    name: 'extractionNamespace.namespaceParseSpec.format',
    label: 'Parse format',
    type: 'string',
    suggestions: ['csv', 'tsv', 'simpleJson', 'customJson'],
    defined: l => deepGet(l, 'extractionNamespace.type') === 'uri',
    required: true,
    info: (
      <>
        <p>The format of the data in the lookup files.</p>
        <p>
          The <Code>simpleJson</Code> lookupParseSpec does not take any parameters. It is simply a
          line delimited JSON file where the field is the key, and the field&apos;s value is the
          value.
        </p>
      </>
    ),
  },

  // CSV + TSV
  {
    name: 'extractionNamespace.namespaceParseSpec.skipHeaderRows',
    type: 'number',
    defaultValue: 0,
    defined: l =>
      deepGet(l, 'extractionNamespace.type') === 'uri' &&
      oneOf(deepGet(l, 'extractionNamespace.namespaceParseSpec.format'), 'csv', 'tsv'),
    info: `Number of header rows to be skipped. The default number of header rows to be skipped is 0.`,
  },
  {
    name: 'extractionNamespace.namespaceParseSpec.hasHeaderRow',
    type: 'boolean',
    defaultValue: false,
    defined: l =>
      deepGet(l, 'extractionNamespace.type') === 'uri' &&
      oneOf(deepGet(l, 'extractionNamespace.namespaceParseSpec.format'), 'csv', 'tsv'),
    info: `A flag to indicate that column information can be extracted from the input files' header row`,
  },
  {
    name: 'extractionNamespace.namespaceParseSpec.columns',
    type: 'string-array',
    placeholder: `["key", "value"]`,
    defined: l =>
      deepGet(l, 'extractionNamespace.type') === 'uri' &&
      oneOf(deepGet(l, 'extractionNamespace.namespaceParseSpec.format'), 'csv', 'tsv'),
    required: l => !deepGet(l, 'extractionNamespace.namespaceParseSpec.hasHeaderRow'),
    info: 'The list of columns in the csv file',
  },
  {
    name: 'extractionNamespace.namespaceParseSpec.keyColumn',
    type: 'string',
    placeholder: '(optional - defaults to the first column)',
    defined: l =>
      deepGet(l, 'extractionNamespace.type') === 'uri' &&
      oneOf(deepGet(l, 'extractionNamespace.namespaceParseSpec.format'), 'csv', 'tsv'),
    info: 'The name of the column containing the key',
  },
  {
    name: 'extractionNamespace.namespaceParseSpec.valueColumn',
    type: 'string',
    placeholder: '(optional - defaults to the second column)',
    defined: l =>
      deepGet(l, 'extractionNamespace.type') === 'uri' &&
      oneOf(deepGet(l, 'extractionNamespace.namespaceParseSpec.format'), 'csv', 'tsv'),
    info: 'The name of the column containing the value',
  },

  // TSV only
  {
    name: 'extractionNamespace.namespaceParseSpec.delimiter',
    type: 'string',
    placeholder: `(optional)`,
    defined: l =>
      deepGet(l, 'extractionNamespace.type') === 'uri' &&
      deepGet(l, 'extractionNamespace.namespaceParseSpec.format') === 'tsv',
  },
  {
    name: 'extractionNamespace.namespaceParseSpec.listDelimiter',
    type: 'string',
    placeholder: `(optional)`,
    defined: l =>
      deepGet(l, 'extractionNamespace.type') === 'uri' &&
      deepGet(l, 'extractionNamespace.namespaceParseSpec.format') === 'tsv',
  },

  // Custom JSON
  {
    name: 'extractionNamespace.namespaceParseSpec.keyFieldName',
    type: 'string',
    placeholder: `key`,
    defined: l =>
      deepGet(l, 'extractionNamespace.type') === 'uri' &&
      deepGet(l, 'extractionNamespace.namespaceParseSpec.format') === 'customJson',
    required: true,
  },
  {
    name: 'extractionNamespace.namespaceParseSpec.valueFieldName',
    type: 'string',
    placeholder: `value`,
    defined: l =>
      deepGet(l, 'extractionNamespace.type') === 'uri' &&
      deepGet(l, 'extractionNamespace.namespaceParseSpec.format') === 'customJson',
    required: true,
  },
  {
    name: 'extractionNamespace.pollPeriod',
    type: 'string',
    defaultValue: '0',
    defined: l => oneOf(deepGet(l, 'extractionNamespace.type'), 'uri', 'jdbc'),
    info: `Period between polling for updates`,
  },

  // JDBC stuff
  {
    name: 'extractionNamespace.connectorConfig.connectURI',
    label: 'Connect URI',
    type: 'string',
    defined: l => deepGet(l, 'extractionNamespace.type') === 'jdbc',
    required: true,
    info: 'Defines the connectURI value on the The connector config to used',
  },
  {
    name: 'extractionNamespace.connectorConfig.user',
    type: 'string',
    defined: l => deepGet(l, 'extractionNamespace.type') === 'jdbc',
    info: 'Defines the user to be used by the connector config',
  },
  {
    name: 'extractionNamespace.connectorConfig.password',
    type: 'string',
    defined: l => deepGet(l, 'extractionNamespace.type') === 'jdbc',
    info: 'Defines the password to be used by the connector config',
  },
  {
    name: 'extractionNamespace.connectorConfig.createTables',
    type: 'boolean',
    defined: l => deepGet(l, 'extractionNamespace.type') === 'jdbc',
    info: 'Should tables be created',
  },
  {
    name: 'extractionNamespace.table',
    type: 'string',
    placeholder: 'some_lookup_table',
    defined: l => deepGet(l, 'extractionNamespace.type') === 'jdbc',
    required: true,
    info: (
      <>
        <p>
          The table which contains the key value pairs. This will become the table value in the SQL
          query:
        </p>
        <p>
          SELECT keyColumn, valueColumn, tsColumn? FROM namespace.<strong>table</strong> WHERE
          filter
        </p>
      </>
    ),
  },
  {
    name: 'extractionNamespace.keyColumn',
    type: 'string',
    placeholder: 'my_key_value',
    defined: l => deepGet(l, 'extractionNamespace.type') === 'jdbc',
    required: true,
    info: (
      <>
        <p>
          The column in the table which contains the keys. This will become the keyColumn value in
          the SQL query:
        </p>
        <p>
          SELECT <strong>keyColumn</strong>, valueColumn, tsColumn? FROM namespace.table WHERE
          filter
        </p>
      </>
    ),
  },
  {
    name: 'extractionNamespace.valueColumn',
    type: 'string',
    placeholder: 'my_column_value',
    defined: l => deepGet(l, 'extractionNamespace.type') === 'jdbc',
    required: true,
    info: (
      <>
        <p>
          The column in table which contains the values. This will become the valueColumn value in
          the SQL query:
        </p>
        <p>
          SELECT keyColumn, <strong>valueColumn</strong>, tsColumn? FROM namespace.table WHERE
          filter
        </p>
      </>
    ),
  },
  {
    name: 'extractionNamespace.filter',
    type: 'string',
    placeholder: '(optional)',
    defined: l => deepGet(l, 'extractionNamespace.type') === 'jdbc',
    info: (
      <>
        <p>
          The filter to be used when selecting lookups, this is used to create a where clause on
          lookup population. This will become the expression filter in the SQL query:
        </p>
        <p>
          SELECT keyColumn, valueColumn, tsColumn? FROM namespace.table WHERE{' '}
          <strong>filter</strong>
        </p>
      </>
    ),
  },
  {
    name: 'extractionNamespace.tsColumn',
    type: 'string',
    label: 'Timestamp column',
    placeholder: '(optional)',
    defined: l => deepGet(l, 'extractionNamespace.type') === 'jdbc',
    info: (
      <>
        <p>
          The column in table which contains when the key was updated. This will become the Value in
          the SQL query:
        </p>
        <p>
          SELECT keyColumn, valueColumn, <strong>tsColumn</strong>? FROM namespace.table WHERE
          filter
        </p>
      </>
    ),
  },

  // Extra cachedNamespace things
  {
    name: 'firstCacheTimeout',
    type: 'number',
    defaultValue: 0,
    defined: typeIs('cachedNamespace'),
    info: `How long to wait (in ms) for the first run of the cache to populate. 0 indicates to not wait`,
  },
  {
    name: 'injective',
    type: 'boolean',
    defaultValue: false,
    defined: typeIs('cachedNamespace'),
    info: `If the underlying map is injective (keys and values are unique) then optimizations can occur internally by setting this to true`,
  },
];

export function isLookupInvalid(
  lookupName: string | undefined,
  lookupVersion: string | undefined,
  lookupTier: string | undefined,
  lookupSpec: Partial<LookupSpec>,
) {
  return (
    !lookupName ||
    !lookupVersion ||
    !lookupTier ||
    !AutoForm.isValidModel(lookupSpec, LOOKUP_FIELDS)
  );
}
