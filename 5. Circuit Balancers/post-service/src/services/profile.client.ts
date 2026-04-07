import { breaker, State } from "../circuit-breaker/circuit-breaker";


if(CircuitBreakerState.OPEN) {
  throw new Error('Circuit breaker is open');
}

if (CircuitBreakerState.CLOSED) {
  fetch('')
}
