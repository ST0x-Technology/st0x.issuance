#!/usr/bin/env nu

# Smoke-tests an already-built issuance-bot image by running the server binary
# the way production does (compose runs `./server`) and asserting it actually
# starts. With no config the binary loads, reaches its own config parsing, and
# exits with "Failed to parse configuration" — its error for the missing
# required env. That marker proves the binary executed and reached application
# code; anything that stops it from running fails to produce it, so the captured
# output is printed for diagnosis.

def main [image: string] {
  let result = (^timeout 30 docker run --rm $image ./server | complete)
  let output = $"($result.stdout)($result.stderr)"

  print $output

  if $result.exit_code == 124 {
    print "::error::server binary timed out after 30s"
    exit 1
  }

  if not ($output | str contains "Failed to parse configuration") {
    print "::error::server binary did not start"
    exit 1
  }

  print "server binary started"
}
