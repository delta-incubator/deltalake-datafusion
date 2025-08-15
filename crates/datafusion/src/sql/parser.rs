use std::collections::VecDeque;

use datafusion::common::{DataFusionError, Diagnostic, Result, Span};
use datafusion::sql::parser::{DFParser, DFParserBuilder, Statement as DFStatement};
use datafusion::sql::sqlparser::dialect::{Dialect, GenericDialect};
use sqlparser::ast::{ObjectName, Value};
use sqlparser::keywords::Keyword;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::{Token, TokenWithSpan, Word};
use url::Url;

use crate::sql::{CreateCatalogStatement, DropCatalogStatement, UnityCatalogStatement};

/// Same as `sqlparser`
const DEFAULT_RECURSION_LIMIT: usize = 50;
const DEFAULT_DIALECT: GenericDialect = GenericDialect {};

// Use `Parser::expected` instead, if possible
macro_rules! parser_err {
    ($MSG:expr $(; diagnostic = $DIAG:expr)?) => {{
        let err = DataFusionError::from(ParserError::ParserError($MSG.to_string()));
        $(
            let err = err.with_diagnostic($DIAG);
        )?
        Err(err)
    }};
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    /// Datafusion SQL Statement.
    DFStatement(Box<DFStatement>),
    UnityCatalog(UnityCatalogStatement),
}

/// Hydrofoil SQL Parser based on [`sqlparser`]
pub struct HFParser<'a> {
    pub parser: DFParser<'a>,
}

pub struct HFParserBuilder<'a> {
    /// The SQL string to parse
    sql: &'a str,
    /// The Dialect to use (defaults to [`GenericDialect`]
    dialect: &'a dyn Dialect,
    /// The recursion limit while parsing
    recursion_limit: usize,
}

impl<'a> HFParserBuilder<'a> {
    /// Create a new parser builder for the specified tokens using the
    /// [`GenericDialect`].
    pub fn new(sql: &'a str) -> Self {
        Self {
            sql,
            dialect: &DEFAULT_DIALECT,
            recursion_limit: DEFAULT_RECURSION_LIMIT,
        }
    }

    /// Adjust the parser builder's dialect. Defaults to [`GenericDialect`]
    pub fn with_dialect(mut self, dialect: &'a dyn Dialect) -> Self {
        self.dialect = dialect;
        self
    }

    /// Adjust the recursion limit of sql parsing.  Defaults to 50
    pub fn with_recursion_limit(mut self, recursion_limit: usize) -> Self {
        self.recursion_limit = recursion_limit;
        self
    }

    pub fn build(self) -> Result<HFParser<'a>, DataFusionError> {
        let parser = DFParserBuilder::new(self.sql)
            .with_dialect(self.dialect)
            .with_recursion_limit(self.recursion_limit)
            .build()?;
        Ok(HFParser { parser })
    }
}

impl<'a> HFParser<'a> {
    /// Parse a sql string into one or [`Statement`]s using the
    /// [`GenericDialect`].
    pub fn parse_sql(sql: &'a str) -> Result<VecDeque<Statement>, DataFusionError> {
        let mut parser = HFParserBuilder::new(sql).build()?;
        parser.parse_statements()
    }

    /// Parse a sql string into one or [`Statement`]s
    pub fn parse_statements(&mut self) -> Result<VecDeque<Statement>, DataFusionError> {
        let mut stmts = VecDeque::new();
        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while self.parser.parser.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if self.parser.parser.peek_token() == Token::EOF {
                break;
            }
            if expecting_statement_delimiter {
                return self.expected("end of statement", self.parser.parser.peek_token());
            }

            let statement = self.parse_statement()?;
            stmts.push_back(statement);
            expecting_statement_delimiter = true;
        }
        Ok(stmts)
    }

    /// Report an unexpected token
    fn expected<T>(&self, expected: &str, found: TokenWithSpan) -> Result<T, DataFusionError> {
        let sql_parser_span = found.span;
        let span = Span::try_from_sqlparser_span(sql_parser_span);
        let diagnostic = Diagnostic::new_error(
            format!("Expected: {expected}, found: {found}{}", found.span.start),
            span,
        );
        parser_err!(
            format!("Expected: {expected}, found: {found}{}", found.span.start);
            diagnostic=
            diagnostic
        )
    }

    /// Parse a new expression
    pub fn parse_statement(&mut self) -> Result<Statement, DataFusionError> {
        match self.parser.parser.peek_token().token {
            Token::Word(w) => {
                match w.keyword {
                    Keyword::CREATE => {
                        self.parser.parser.next_token(); // CREATE
                        self.parse_create()
                    }
                    // NOTE: we must not consume DROP here, since we delegate to sqlparser-rs
                    // for regular parsing of DROP statements
                    Keyword::DROP => self.parse_drop(),
                    _ => {
                        // use sqlparser-rs parser
                        self.parse_and_handle_statement()
                    }
                }
            }
            _ => {
                // use the native parser
                self.parse_and_handle_statement()
            }
        }
    }

    /// Parse a SQL `CREATE` statement handling `CREATE EXTERNAL TABLE`
    pub fn parse_create(&mut self) -> Result<Statement, DataFusionError> {
        if self.parser.parser.parse_keyword(Keyword::CATALOG) {
            self.parse_create_catalog()
        } else if self.parser.parser.parse_keyword(Keyword::CONNECTION) {
            self.parse_create_connection()
        } else if self
            .parser
            .parser
            .parse_keywords(&[Keyword::FOREIGN, Keyword::CATALOG])
        {
            self.parse_create_foreign_catalog()
        } else if self.parser.parser.parse_keyword(Keyword::LOCATION) {
            self.parse_create_location()
        } else if self.parser.parser.parse_keyword(Keyword::SCHEMA) {
            self.parse_create_schema()
        } else if self.parser.parser.parse_keyword(Keyword::SHARE) {
            self.parse_create_share()
        } else {
            Ok(Statement::DFStatement(Box::from(
                self.parser.parse_create()?,
            )))
        }
    }

    fn parse_create_catalog(&mut self) -> Result<Statement, DataFusionError> {
        let if_not_exists =
            self.parser
                .parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let catalog_name = self.parser.parser.parse_object_name(false)?;
        if catalog_name.0.len() != 1 {
            return parser_err!("Expected catalog name to be a single-part identifier (<catalog>)");
        }

        #[derive(Default)]
        struct Builder {
            pub using_share: Option<ObjectName>,
            pub managed_location: Option<Url>,
            pub default_collation: Option<String>,
            pub comment: Option<String>,
            pub options: Option<Vec<(String, Value)>>,
        }
        let mut builder = Builder::default();

        loop {
            if let Some(keyword) = self.parser.parser.parse_one_of_keywords(&[
                Keyword::USING,
                Keyword::MANAGED,
                Keyword::COMMENT,
                Keyword::DEFAULT,
                Keyword::OPTIONS,
            ]) {
                match keyword {
                    Keyword::USING => {
                        self.parser.parser.expect_keyword(Keyword::SHARE)?;
                        ensure_not_set(&builder.using_share, "USING SHARE")?;
                        let share_name = self.parser.parser.parse_object_name(false)?;
                        if share_name.0.len() != 2 {
                            return parser_err!(
                                "Expected share name to be a two-part identifier (<provider>.<share>)"
                            );
                        }
                        builder.using_share = Some(share_name);
                    }
                    Keyword::MANAGED => {
                        self.parser.parser.expect_keyword(Keyword::LOCATION)?;
                        ensure_not_set(&builder.managed_location, "MANAGED LOCATION")?;
                        let managed_location = self.parser.parser.parse_literal_string()?;
                        let Ok(managed_location) = Url::parse(&managed_location) else {
                            return parser_err!("Expected managed location to be a valid URL");
                        };
                        builder.managed_location = Some(managed_location);
                    }
                    Keyword::DEFAULT => {
                        self.parser.parser.expect_keyword(Keyword::COLLATION)?;
                        ensure_not_set(&builder.default_collation, "DEFAULT COLLATION")?;
                        let default = self.parser.parser.parse_literal_string()?;
                        builder.default_collation = Some(default);
                    }
                    Keyword::COMMENT => {
                        ensure_not_set(&builder.comment, "COMMENT")?;
                        let comment = self.parser.parser.parse_literal_string()?;
                        builder.comment = Some(comment);
                    }
                    Keyword::OPTIONS => {
                        ensure_not_set(&builder.options, "OPTIONS")?;
                        builder.options = Some(self.parse_value_options()?);
                    }
                    _ => {
                        unreachable!()
                    }
                }
            } else {
                let token = self.parser.parser.next_token();
                if token == Token::EOF || token == Token::SemiColon {
                    break;
                } else {
                    return self.expected("end of statement or ;", token)?;
                }
            }
        }

        if builder.using_share.is_some() && builder.managed_location.is_some() {
            return parser_err!("USING SHARE and MANAGED LOCATION are mutually exclusive.");
        }

        Ok(Statement::UnityCatalog(
            CreateCatalogStatement {
                name: catalog_name,
                if_not_exists,
                using_share: builder.using_share,
                managed_location: builder.managed_location,
                default_collation: builder.default_collation,
                comment: builder.comment,
                options: builder.options,
            }
            .into(),
        ))
    }

    fn parse_create_foreign_catalog(&mut self) -> Result<Statement, DataFusionError> {
        todo!("Implement parse_create_foreign_catalog")
    }

    fn parse_create_connection(&mut self) -> Result<Statement, DataFusionError> {
        todo!("Implement parse_create_connection")
    }

    fn parse_create_location(&mut self) -> Result<Statement, DataFusionError> {
        todo!("Implement parse_create_location")
    }

    fn parse_create_schema(&mut self) -> Result<Statement, DataFusionError> {
        todo!("Implement parse_create_schema")
    }

    fn parse_create_share(&mut self) -> Result<Statement, DataFusionError> {
        todo!("Implement parse_create_share")
    }

    pub fn parse_drop(&mut self) -> Result<Statement, DataFusionError> {
        if self
            .parser
            .parser
            .parse_keywords(&[Keyword::DROP, Keyword::CATALOG])
        {
            self.parse_drop_catalog()
        } else {
            self.parse_and_handle_statement()
        }
    }

    fn parse_drop_catalog(&mut self) -> Result<Statement, DataFusionError> {
        let if_exists = self
            .parser
            .parser
            .parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let name = self.parser.parser.parse_object_name(false)?;
        if name.0.len() != 1 {
            return parser_err!("Expected catalog name to be a single-part identifier (<catalog>)");
        }
        let cascade = self.parser.parser.parse_keyword(Keyword::CASCADE);
        Ok(Statement::UnityCatalog(
            DropCatalogStatement {
                name,
                if_exists,
                cascade,
            }
            .into(),
        ))
    }

    /// Parses (key value) style options into a map of String --> [`Value`].
    ///
    /// This method supports keywords as key names as well as multiple
    /// value types such as Numbers as well as Strings.
    fn parse_value_options(&mut self) -> Result<Vec<(String, Value)>, DataFusionError> {
        let mut options = vec![];
        self.parser.parser.expect_token(&Token::LParen)?;

        loop {
            let key = self.parse_option_key()?;
            let value = self.parse_option_value()?;
            options.push((key, value));
            let comma = self.parser.parser.consume_token(&Token::Comma);
            if self.parser.parser.consume_token(&Token::RParen) {
                // Allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected(
                    "',' or ')' after option definition",
                    self.parser.parser.peek_token(),
                );
            }
        }
        Ok(options)
    }

    /// Parse the next token as a key name for an option list
    ///
    /// Note this is different than [`parse_literal_string`]
    /// because it allows keywords as well as other non words
    ///
    /// [`parse_literal_string`]: sqlparser::parser::Parser::parse_literal_string
    pub fn parse_option_key(&mut self) -> Result<String, DataFusionError> {
        let next_token = self.parser.parser.next_token();
        match next_token.token {
            Token::Word(Word { value, .. }) => {
                let mut parts = vec![value];
                while self.parser.parser.consume_token(&Token::Period) {
                    let next_token = self.parser.parser.next_token();
                    if let Token::Word(Word { value, .. }) = next_token.token {
                        parts.push(value);
                    } else {
                        // Unquoted namespaced keys have to conform to the syntax
                        // "<WORD>[\.<WORD>]*". If we have a key that breaks this
                        // pattern, error out:
                        return self.expected("key name", next_token);
                    }
                }
                Ok(parts.join("."))
            }
            Token::SingleQuotedString(s) => Ok(s),
            Token::DoubleQuotedString(s) => Ok(s),
            Token::EscapedStringLiteral(s) => Ok(s),
            _ => self.expected("key name", next_token),
        }
    }

    /// Parse the next token as a value for an option list
    ///
    /// Note this is different than [`parse_value`] as it allows any
    /// word or keyword in this location.
    ///
    /// [`parse_value`]: sqlparser::parser::Parser::parse_value
    pub fn parse_option_value(&mut self) -> Result<Value, DataFusionError> {
        let next_token = self.parser.parser.next_token();
        match next_token.token {
            // e.g. things like "snappy" or "gzip" that may be keywords
            Token::Word(word) => Ok(Value::SingleQuotedString(word.value)),
            Token::SingleQuotedString(s) => Ok(Value::SingleQuotedString(s)),
            Token::DoubleQuotedString(s) => Ok(Value::DoubleQuotedString(s)),
            Token::EscapedStringLiteral(s) => Ok(Value::EscapedStringLiteral(s)),
            Token::Number(n, l) => Ok(Value::Number(n, l)),
            _ => self.expected("string or numeric value", next_token),
        }
    }

    /// Helper method to parse a statement and handle errors consistently, especially for recursion limits
    fn parse_and_handle_statement(&mut self) -> Result<Statement, DataFusionError> {
        self.parser
            .parser
            .parse_statement()
            .map(|stmt| Statement::DFStatement(Box::from(DFStatement::Statement(Box::from(stmt)))))
            .map_err(|e| match e {
                ParserError::RecursionLimitExceeded => DataFusionError::SQL(
                    Box::new(ParserError::RecursionLimitExceeded),
                    Some(format!(" (current limit)")),
                ),
                other => DataFusionError::SQL(Box::new(other), None),
            })
    }
}

fn ensure_not_set<T>(field: &Option<T>, name: &str) -> Result<(), DataFusionError> {
    if field.is_some() {
        parser_err!(format!("{name} specified more than once",))?
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::Ident;

    use super::*;

    #[test]
    fn test_parse_create_catalog() {
        let sql = "CREATE CATALOG IF NOT EXISTS my_catalog";
        let statements = HFParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let expected_name: ObjectName = vec![Ident::new("my_catalog")].into();
        assert!(matches!(
            &statements[0],
            Statement::UnityCatalog(UnityCatalogStatement::CreateCatalog(CreateCatalogStatement {
                name,
                if_not_exists: true,
                using_share: None,
                managed_location: None,
                default_collation: None,
                comment: None,
                options: None,
                ..
            })) if name == &expected_name
        ));

        let sql = "CREATE CATALOG my_catalog USING SHARE provider.share";
        let statements = HFParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let expected_name: ObjectName = vec![Ident::new("provider"), Ident::new("share")].into();
        assert!(matches!(
            &statements[0],
            Statement::UnityCatalog(UnityCatalogStatement::CreateCatalog(CreateCatalogStatement {
                using_share,
                ..
            })) if using_share == &Some(expected_name)
        ));

        let sql = "CREATE CATALOG my_catalog MANAGED LOCATION 's3://my-bucket/my_catalog'";
        let statements = HFParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let expected_location = Url::parse("s3://my-bucket/my_catalog").unwrap();
        assert!(matches!(
            &statements[0],
            Statement::UnityCatalog(UnityCatalogStatement::CreateCatalog(CreateCatalogStatement {
                managed_location,
                ..
            })) if managed_location == &Some(expected_location)
        ));
    }
}
