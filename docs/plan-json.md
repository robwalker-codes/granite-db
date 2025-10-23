# EXPLAIN JSON format

GraniteDB exposes a stable JSON payload for physical plans via `Database.ExplainJSON` and `granitectl explain --json`. The schema is versioned so clients can evolve alongside the engine.

## Schema v1

```json
{
  "version": 1,
  "physical": {
    "node": "Project|SeqScan|IndexScan|Filter|Sort|Limit|HashAgg|NestedLoopJoin|HashJoin|IndexNestedLoopJoin",
    "props": {
      "table": "orders",
      "index": "idx_orders_total",
      "predicate": "total > 50",
      "orderBy": [
        {"expr": "total", "dir": "ASC"}
      ],
      "limit": 10,
      "offset": 0,
      "groupKeys": ["customer_id"],
      "aggs": [
        {"fn": "SUM", "expr": "total", "alias": "spend"}
      ],
      "joinType": "Inner|Left",
      "condition": "c.id = o.customer_id",
      "usingIndexOrder": false
    },
    "children": ["… nested nodes …"]
  },
  "text": "- Project\n  - SeqScan map[table:orders]"
}
```

* `version` communicates the payload version. Breaking schema changes increment the number.
* `physical` describes the operator tree. Every node contains the operator `node` name, optional `props`, and optional `children`.
* The `props` object is omitted when a node has no applicable properties. Individual fields only appear when the corresponding attribute is present in the plan (for example, `limit` and `offset` only appear on limit nodes).
* `text` matches the compact tree emitted by `granitectl explain` to ease snapshot testing.

## Example: filter, sort, limit

```json
{
  "version": 1,
  "physical": {
    "node": "Project",
    "children": [
      {
        "node": "Limit",
        "props": {"limit": 1, "offset": 0},
        "children": [
          {
            "node": "Sort",
            "props": {"orderBy": [{"expr": "total", "dir": "ASC"}]},
            "children": [
              {
                "node": "Filter",
                "props": {"predicate": "total > 50"},
                "children": [
                  {
                    "node": "SeqScan",
                    "props": {"table": "orders"}
                  }
                ]
              }
            ]
          }
        ]
      }
    ]
  },
  "text": "- Project\n  - Limit map[limit:1 offset:0]\n    - Sort map[orderBy:[map[dir:ASC expr:total]]]\n      - Filter map[predicate:total > 50]\n        - SeqScan map[table:orders]"
}
```

## Example: grouped aggregation with join

```json
{
  "version": 1,
  "physical": {
    "node": "Project",
    "children": [
      {
        "node": "HashAgg",
        "props": {
          "groupKeys": ["customer_id"],
          "aggs": [
            {"fn": "SUM", "expr": "total", "alias": "spend"}
          ]
        },
        "children": [
          {
            "node": "HashJoin",
            "props": {
              "joinType": "Inner",
              "condition": "c.id = o.customer_id"
            },
            "children": [
              {"node": "SeqScan", "props": {"table": "customers"}},
              {"node": "SeqScan", "props": {"table": "orders"}}
            ]
          }
        ]
      }
    ]
  },
  "text": "- Project\n  - HashAgg map[aggs:[map[alias:spend expr:total fn:SUM]] groupKeys:[customer_id]]\n    - HashJoin map[condition:c.id = o.customer_id joinType:Inner]\n      - SeqScan map[table:customers]\n      - SeqScan map[table:orders]"
}
```

The examples highlight how each operator contributes only the properties relevant to that step. Future versions may extend `props` to cover additional attributes while keeping existing field names stable.

