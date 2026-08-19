from typing import Literal


GenerationFailureClass = Literal["transport", "validation", "canceled"]
GenerationRetryKind = Literal["initial", "automatic", "manual"]
