#include <bits/stdc++.h>

using namespace std;

// ensure the stream is well-formed, i.e. does not double-delete or
// double-insert.
int main() {
  int n = 20;
  int m = 183;

  vector<vector<bool>> arr(n,vector<bool>(n,false));
  int t,a,b;
  for (int i = 0; i < m; ++i) {
    cin >> t >> a >> b;
    if (t != arr[a][b]) {
      std::cout << "Failure" << std::endl;
      return 0;
    }
    arr[a][b] = !arr[a][b];
  }
  int edges = 0;
  for (int i = 0; i < n; ++i) {
    for (int j = i + 1; j < n; ++j) {
      edges += arr[i][j];
    }
  }
  std::cout << "OK. Processed "<< m << " edges and found "
  << edges << " out of " << n*(n-1)/2 << " edges." << std::endl;
  return 0;
}
