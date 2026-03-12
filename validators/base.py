from dataclasses import dataclass
import pandas as pd

@dataclass
class ValidationError:
    code: str
    message: str

@dataclass
class ValidationResult:
    ok: bool
    errors: list[ValidationError]

class BaseValidator:
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        raise NotImplementedError