//! Local configuration intentions, independent of command-line syntax and file I/O.
#[derive(Clone)]
pub enum Credential {
    Bearer {
        environment: String,
    },
    Basic {
        username: String,
        environment: String,
    },
}

pub enum AccessCommand {
    List,
    Validate,
    Check {
        principal: String,
        alias: String,
        operation: String,
    },
    Init,
    AddPrincipal {
        id: String,
        credential: Credential,
    },
    SetCredential {
        id: String,
        credential: Credential,
    },
    RemovePrincipal {
        id: String,
    },
    AddShare {
        alias: String,
        path: String,
        access: String,
        public: bool,
    },
    SetShare {
        alias: String,
        access: Option<String>,
        public: Option<bool>,
    },
    RemoveShare {
        alias: String,
    },
    Grant {
        principal: String,
        alias: String,
        access: String,
        execute: bool,
    },
    Revoke {
        principal: String,
        alias: String,
    },
}

impl AccessCommand {
    pub fn changes_config(&self) -> bool {
        !matches!(self, Self::List | Self::Validate | Self::Check { .. })
    }
}
