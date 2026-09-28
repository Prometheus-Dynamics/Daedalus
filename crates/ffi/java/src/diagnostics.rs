//! Classification of common Java worker runtime failures.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JavaRuntimeDiagnostic {
    pub kind: JavaRuntimeDiagnosticKind,
    pub message: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JavaRuntimeDiagnosticKind {
    ClassNotFound,
    MethodNotFound,
    NativeLibraryLoad,
    Invocation,
    Process,
}

pub fn classify_java_runtime_diagnostic(
    stderr: impl AsRef<str>,
    fallback: impl Into<String>,
) -> JavaRuntimeDiagnostic {
    let stderr = stderr.as_ref();
    let message = if stderr.trim().is_empty() {
        fallback.into()
    } else {
        stderr.trim().to_string()
    };
    let kind = if contains_any(
        stderr,
        &[
            "ClassNotFoundException",
            "NoClassDefFoundError",
            "Could not find or load main class",
        ],
    ) {
        JavaRuntimeDiagnosticKind::ClassNotFound
    } else if contains_any(
        stderr,
        &[
            "NoSuchMethodException",
            "NoSuchMethodError",
            "method not found",
        ],
    ) {
        JavaRuntimeDiagnosticKind::MethodNotFound
    } else if contains_any(
        stderr,
        &[
            "UnsatisfiedLinkError",
            "java.library.path",
            " in java.library.path",
        ],
    ) {
        JavaRuntimeDiagnosticKind::NativeLibraryLoad
    } else if contains_any(
        stderr,
        &[
            "InvocationTargetException",
            "Exception in thread",
            "RuntimeException",
        ],
    ) {
        JavaRuntimeDiagnosticKind::Invocation
    } else {
        JavaRuntimeDiagnosticKind::Process
    };

    JavaRuntimeDiagnostic { kind, message }
}

fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(needle))
}
