! SPDX-License-Identifier: PMPL-1.0
! Form.Runtime - FDQL Parser and Executor
!
! FDQL (FormDB Query Language) implementation using PEG parsing.

USING: accessors arrays assocs combinators combinators.short-circuit
continuations generalizations io json kernel locals math math.parser
peg peg.ebnf sequences splitting strings unicode ;

IN: fdql

! ============================================================
! AST Node Types
! ============================================================

TUPLE: fdql-insert collection document provenance ;
TUPLE: fdql-select fields collection where-clause edge-clause limit-clause with-provenance? ;
TUPLE: fdql-update collection assignments where-clause provenance ;
TUPLE: fdql-delete collection where-clause provenance ;
TUPLE: fdql-create collection fields schema ;
TUPLE: fdql-drop collection provenance ;
TUPLE: fdql-explain inner-stmt ;
TUPLE: fdql-introspect target arg ;

TUPLE: edge-clause type direction depth where ;
TUPLE: where-clause expression ;
TUPLE: limit-clause limit offset ;

TUPLE: comparison field op value ;
TUPLE: binary-expr left op right ;

! ============================================================
! Tokenizer
! ============================================================

: skip-whitespace ( str -- str' )
    [ " \t\n\r" member? not ] find drop "" or ;

: keyword? ( str -- ? )
    >upper {
        "SELECT" "FROM" "WHERE" "INSERT" "INTO" "UPDATE"
        "DELETE" "SET" "CREATE" "DROP" "COLLECTION"
        "WITH" "PROVENANCE" "LIMIT" "OFFSET"
        "TRAVERSE" "OUTBOUND" "INBOUND" "ANY" "DEPTH"
        "EXPLAIN" "INTROSPECT" "SCHEMA" "CONSTRAINTS"
        "JOURNAL" "SINCE" "COLLECTIONS"
        "AND" "OR" "NOT" "NULL" "TRUE" "FALSE"
        "STRING" "INTEGER" "FLOAT" "BOOLEAN" "TIMESTAMP"
        "JSON" "PROMPT_SCORE" "UNIQUE" "CHECK" "REFERENCES"
        "LIKE" "IN" "CONTAINS"
    } member? ;

: split-tokens ( str -- tokens )
    ! Simple tokenizer - splits on whitespace and punctuation
    " \t\n\r" split harvest
    [ "," = not ] filter ;

! ============================================================
! Parser Combinators (Simplified)
! ============================================================

ERROR: fdql-parse-error message position ;

: expect-token ( tokens expected -- tokens' )
    swap unclip-slice
    pick >upper = [ drop ] [
        "Expected '" "'" surround fdql-parse-error
    ] if ;

: peek-token ( tokens -- token/f )
    [ first ] [ drop f ] if-empty ;

: consume-token ( tokens -- tokens' token )
    unclip-slice ;  ! Returns ( rest first ) = ( tokens' token ), token on top

: try-consume ( tokens expected -- tokens' matched? )
    over peek-token dup [
        >upper = [
            swap unclip-slice drop t
        ] [
            drop f
        ] if
    ] [
        2drop f
    ] if ;

! ============================================================
! Parse Primitives
! ============================================================

: parse-identifier ( tokens -- tokens' identifier )
    consume-token
    dup keyword? [ "Unexpected keyword" fdql-parse-error ] when ;

: parse-json-value ( tokens -- tokens' value )
    ! Simplified: just consume until end of JSON
    consume-token
    dup "{" = [
        drop
        ! Parse JSON object
        H{ } clone
        [ over peek-token "}" = not ] [
            swap consume-token drop  ! key
            swap consume-token drop  ! :
            swap consume-token       ! value
            ! Would need full JSON parsing here
            2drop
        ] while
        swap consume-token drop  ! }
        swap
    ] [
        ! It's a simple value
        swap
    ] if ;

: parse-string-literal ( tokens -- tokens' string )
    consume-token
    ! Remove quotes if present
    dup first CHAR: " = [
        1 tail* dup length 1 - head*
    ] when ;

! ============================================================
! Parse Statements
! ============================================================

: parse-collection-name ( tokens -- tokens' name )
    parse-identifier ;

: parse-field-list ( tokens -- tokens' fields )
    ! Collect field names until we see "FROM"
    V{ } clone swap  ! ( accum tokens )
    [ dup peek-token "FROM" = not ] [
        consume-token           ! ( accum tokens' field )
        [ swap ] dip            ! ( tokens' accum field )
        suffix                  ! ( tokens' accum' )
        swap                    ! ( accum' tokens' )
        dup peek-token "," = [ consume-token drop ] when
    ] while
    swap >array ;  ! ( tokens' fields )

: parse-where-clause ( tokens -- tokens' where/f )
    "WHERE" try-consume [
        ! Parse expression: field op value
        parse-identifier              ! ( tokens' field )
        [ consume-token ] dip         ! ( tokens'' op field )
        [ consume-token ] dip         ! ( tokens''' value op field )
        swap rot                      ! ( tokens''' field op value )
        comparison boa                ! ( tokens''' comparison )
        where-clause boa              ! ( tokens''' where )
    ] [
        f
    ] if ;

:: parse-edge-clause ( tokens -- tokens' edge/f )
    "TRAVERSE" tokens try-consume :> ( tokens' matched? )
    matched? [
        tokens' parse-identifier :> ( tokens'' edge-type )
        tokens'' consume-token :> ( tokens''' dir-raw )
        dir-raw >upper :> direction
        "DEPTH" tokens''' try-consume :> ( tokens'''' has-depth? )
        has-depth? [
            tokens'''' consume-token string>number :> ( tokens''''' depth )
            tokens'''''
            edge-type direction depth f edge-clause boa
        ] [
            tokens''''
            edge-type direction 1 f edge-clause boa
        ] if
    ] [
        tokens f
    ] if ;

:: parse-limit-clause ( tokens -- tokens' limit/f )
    "LIMIT" tokens try-consume :> ( tokens' matched? )
    matched? [
        tokens' consume-token string>number :> ( tokens'' limit )
        "OFFSET" tokens'' try-consume :> ( tokens''' has-offset? )
        has-offset? [
            tokens''' consume-token string>number :> ( tokens'''' offset )
            tokens''''
            limit offset limit-clause boa
        ] [
            tokens'''
            limit 0 limit-clause boa
        ] if
    ] [
        tokens f
    ] if ;

: parse-provenance-clause ( tokens -- tokens' prov/f )
    "WITH" try-consume [
        "PROVENANCE" expect-token
        ! Parse JSON object
        consume-token drop  ! {
        H{ } clone          ! placeholder
        [ over peek-token "}" = not ] [
            swap consume-token drop  ! skip tokens until }
        ] while
        swap consume-token drop  ! }
        swap
    ] [
        f
    ] if ;

! ============================================================
! Statement Parsers
! ============================================================

:: parse-insert ( tokens -- tokens' ast )
    "INTO" tokens expect-token parse-collection-name :> ( tokens' collection )
    ! Parse document body (simplified - just consume JSON)
    tokens' consume-token drop :> tokens''  ! consume {
    H{ } clone :> doc
    tokens'' [ dup peek-token "}" = not ] [
        consume-token drop
    ] while :> tokens'''
    tokens''' consume-token drop :> tokens''''  ! consume }
    ! Parse provenance
    tokens'''' parse-provenance-clause :> ( final-tokens prov )
    final-tokens
    collection doc prov fdql-insert boa ;

: parse-select ( tokens -- tokens' ast )
    ! Simplified: just parse "SELECT fields FROM collection"
    ! Parse field list - returns ( tokens' fields )
    dup peek-token "*" = [
        consume-token drop { "*" }
    ] [
        parse-field-list
    ] if
    swap  ! ( fields tokens' )

    ! FROM collection - returns ( fields tokens'' collection )
    "FROM" expect-token
    parse-collection-name  ! ( fields tokens'' collection )

    ! Skip optional clauses for now - just use defaults
    ! fdql-select needs: fields collection where-clause edge-clause limit-clause with-provenance?
    f f f f  ! ( fields tokens'' collection f f f f )

    ! Rearrange: need ( tokens'' fields collection where edge lim prov )
    ! Have: ( fields tokens'' collection f f f f )
    ! fields is deepest, tokens'' is second, collection third, then 4 f's
    [ [ [ [ [ 5 nrot ] dip ] dip ] dip ] dip ] dip  ! move tokens'' to bottom
    ! Now: ( tokens'' fields collection f f f f )
    fdql-select boa ;

:: parse-update ( tokens -- tokens' ast )
    tokens parse-collection-name :> ( tokens' collection )
    "SET" tokens' expect-token :> tokens''
    ! Parse assignments (simplified)
    V{ } clone :> assignments
    tokens'' [ dup peek-token "WHERE" = not ] [
        parse-identifier :> ( t field )
        t consume-token drop :> t'   ! =
        t' consume-token :> ( t'' value )
        field value 2array assignments push
        t'' dup peek-token "," = [ consume-token drop ] when
    ] while :> tokens'''
    tokens''' parse-where-clause :> ( tokens'''' where )
    tokens'''' parse-provenance-clause :> ( final-tokens prov )
    final-tokens
    collection assignments >array where prov fdql-update boa ;

:: parse-delete ( tokens -- tokens' ast )
    "FROM" tokens expect-token parse-collection-name :> ( tokens' collection )
    tokens' parse-where-clause :> ( tokens'' where )
    tokens'' parse-provenance-clause :> ( final-tokens prov )
    final-tokens
    collection where prov fdql-delete boa ;

:: parse-create ( tokens -- tokens' ast )
    "COLLECTION" tokens expect-token parse-collection-name :> ( tokens' collection )
    ! Parse optional field definitions
    tokens' peek-token "(" = [
        tokens' consume-token drop :> tokens''  ! consume (
        V{ } clone :> field-defs
        tokens'' [ dup peek-token ")" = not ] [
            parse-identifier :> ( t field )
            t consume-token :> ( t' type )
            field type 2array field-defs push
            t' dup peek-token "," = [ consume-token drop ] when
        ] while :> tokens'''
        tokens''' consume-token drop :> tokens''''  ! consume )
        tokens'''' field-defs >array
    ] [
        tokens' { }
    ] if :> ( tokens''''' fields )
    ! Parse optional schema
    "WITH" tokens''''' try-consume :> ( tokens'''''' has-schema? )
    has-schema? [
        "SCHEMA" tokens'''''' expect-token :> tokens'''''''
        tokens'''''''
        collection fields H{ } clone fdql-create boa  ! placeholder schema
    ] [
        tokens''''''
        collection fields f fdql-create boa
    ] if ;

:: parse-drop ( tokens -- tokens' ast )
    "COLLECTION" tokens expect-token parse-collection-name :> ( tokens' collection )
    tokens' parse-provenance-clause :> ( final-tokens prov )
    final-tokens
    collection prov fdql-drop boa ;

:: parse-introspect-target ( tokens -- tokens' target arg )
    tokens consume-token :> ( tokens' target-raw )
    target-raw >upper :> target
    target "JOURNAL" = [
        "SINCE" tokens' try-consume :> ( tokens'' has-since? )
        has-since? [
            tokens'' consume-token string>number :> ( tokens''' since-val )
            tokens''' target since-val
        ] [
            tokens'' target 0
        ] if
    ] [
        target "SCHEMA" = target "CONSTRAINTS" = or [
            tokens' peek-token :> next-tok
            next-tok keyword? not next-tok and [
                tokens' parse-identifier :> ( tokens'' arg )
                tokens'' target arg
            ] [
                tokens' target f
            ] if
        ] [
            tokens' target f
        ] if
    ] if ;

: parse-introspect ( tokens -- tokens' ast )
    parse-introspect-target
    fdql-introspect boa ;

DEFER: parse-statement

: parse-explain ( tokens -- tokens' ast )
    parse-statement
    fdql-explain boa ;

: parse-statement ( tokens -- tokens' ast )
    consume-token >upper {  ! After consume-token: ( tokens' token ), >upper: ( tokens' TOKEN )
        { "INSERT" [ parse-insert ] }
        { "SELECT" [ parse-select ] }
        { "UPDATE" [ parse-update ] }
        { "DELETE" [ parse-delete ] }
        { "CREATE" [ parse-create ] }
        { "DROP" [ parse-drop ] }
        { "EXPLAIN" [ parse-explain ] }
        { "INTROSPECT" [ parse-introspect ] }
        [ "Unknown statement type" fdql-parse-error ]
    } case ;

! ============================================================
! Main Parser Entry Point
! ============================================================

: parse-fdql ( str -- ast )
    ! Remove trailing semicolon if present
    dup ";" tail? [ but-last ] when
    ! Remove comments
    "\n" split
    [
        "--" split1 drop ! Remove line comments
        "" or
    ] map
    " " join
    ! Tokenize
    split-tokens
    ! Parse
    parse-statement
    ! Should have consumed all tokens
    nip ;

! ============================================================
! Query Execution (Stubs)
! ============================================================

GENERIC: execute-fdql ( ast -- result )

M: fdql-insert execute-fdql
    [
        H{
            { "status" "ok" }
            { "document_id" "doc_generated" }
        }
    ] [ collection>> ] bi
    "collection" pick set-at ;

M: fdql-select execute-fdql
    [
        H{
            { "status" "ok" }
            { "rows" { } }
            { "count" 0 }
        }
    ] [ collection>> ] bi
    "collection" pick set-at ;

M: fdql-update execute-fdql
    H{
        { "status" "ok" }
        { "modified_count" 0 }
    } ;

M: fdql-delete execute-fdql
    H{
        { "status" "ok" }
        { "deleted_count" 0 }
    } ;

M: fdql-create execute-fdql
    [
        H{
            { "status" "ok" }
            { "schema_version" 1 }
        }
    ] [ collection>> ] bi
    "collection" pick set-at ;

M: fdql-drop execute-fdql
    H{
        { "status" "ok" }
    } ;

M: fdql-explain execute-fdql
    inner-stmt>> execute-fdql
    [
        H{
            { "status" "ok" }
            { "plan" H{
                { "type" "SCAN" }
                { "estimated_rows" 0 }
            } }
        }
    ] dip
    "result" pick set-at ;

M: fdql-introspect execute-fdql
    target>> {
        { "SCHEMA" [
            H{
                { "status" "ok" }
                { "fields" { } }
            }
        ] }
        { "CONSTRAINTS" [
            H{
                { "status" "ok" }
                { "constraints" { } }
            }
        ] }
        { "COLLECTIONS" [
            H{
                { "status" "ok" }
                { "collections" { } }
            }
        ] }
        { "JOURNAL" [
            H{
                { "status" "ok" }
                { "entries" { } }
            }
        ] }
        [ drop H{ { "status" "error" } { "message" "Unknown introspect target" } } ]
    } case ;

! ============================================================
! Public API
! ============================================================

: run-fdql ( str -- result )
    parse-fdql execute-fdql ;

: explain-fdql ( str -- plan )
    "EXPLAIN " prepend
    run-fdql ;
