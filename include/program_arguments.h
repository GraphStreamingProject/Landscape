#include <string>
#include <vector>
#include <functional>
#include <unordered_map>

// Result of Parsing the argument
struct ArgumentResult {
  bool parsed_correctly; // was the data accurately parsed?
  std::string err = "ERROR: Unknown error!"; // if incorrect parse. Error goes here

  // depending on the type of the argument one of the following
  // will hold the argument result
  int         int_result;
  float       flt_result;
  std::string str_result;
};

// Default INT parser
template<int low_bound, int up_bound> ArgumentResult int_parser(char *arg) {
  ArgumentResult result;
  result.int_result = std::atoi(arg);
  if (result.int_result < low_bound || result.int_result > up_bound) {
    result.err = "Value " + std::string(arg) + " is out of bounds expected between " 
                 + std::to_string(low_bound) + " and " + std::to_string(up_bound);
    result.parsed_correctly = false;
  } else
    result.parsed_correctly = true;
  
  return result;
}

// Default FLOAT parser
template<int low_bound, int up_bound> ArgumentResult flt_parser(char *arg) {
  ArgumentResult result;
  result.flt_result = std::atof(arg);
  if (result.flt_result < low_bound || result.flt_result > up_bound) {
    result.err = "Value " + std::string(arg) + " is out of bounds expected between " 
                 + std::to_string(low_bound) + " and " + std::to_string(up_bound);
    result.parsed_correctly = false;
  } else
    result.parsed_correctly = true;
  return result;
}

// Default STRING parser
ArgumentResult str_parser(char *arg) {
  ArgumentResult result;
  result.str_result = arg;
  result.parsed_correctly = true;
  return result;
}

// Optional argument parser
ArgumentResult opt_parser(char *arg) {
  ArgumentResult result;
  result.str_result = arg;
  result.parsed_correctly = true;
  // TODO: Deal with arguments to optional args (ie --burst 10)
  return result;
}

class ArgumentDefinition {
private:
  std::string name;
  std::string help;
  std::function<ArgumentResult (char *)> parser;
  bool optional;

  friend class ProgramArguments;
public:
  ArgumentDefinition(std::string n, std::string h, std::function<ArgumentResult (char *)> p, bool opt=false) :
   name(n), help(h), parser(p), optional(opt) {};
  ArgumentDefinition() = default;
  ArgumentDefinition(const ArgumentDefinition &) = default;
  ArgumentDefinition(ArgumentDefinition &&) = default;
  ArgumentDefinition & operator=(const ArgumentDefinition &) = default;
};

class ProgramArguments {
private:
  std::vector<ArgumentDefinition> argument_list;
  std::unordered_map<std::string, ArgumentDefinition> opt_arguments;

  void error(std::string err_msg) {
    // got an error. Print help for each argument in the list
    std::cout << "Error parsing arguments! USAGE:" << std::endl;
    for (auto arg : argument_list)
      std::cout << " " << arg.name << ": " << arg.help << std::endl;
    for (const auto opt : opt_arguments)
      std::cout << " --" << opt.second.name << ": " << opt.second.help << std::endl;
    throw std::invalid_argument(err_msg);
  }
public:
  ProgramArguments(std::vector<ArgumentDefinition> arg_list) : argument_list(arg_list) {
    // Setup optional arguments map
    for (auto iter = argument_list.begin(); iter != argument_list.end(); ) {
      ArgumentDefinition arg = *iter;
      if (arg.optional) {
        opt_arguments[arg.name] = arg;
        iter = argument_list.erase(iter);
      } else
        ++iter;
    }
  };
  
  // parse the arguments and return the result in a map
  std::unordered_map<std::string, ArgumentResult> parse_arguments(int argc, char **argv) {
    if (argc - 1 < (int) argument_list.size())
      error("Too few arguments! Require at least: " + std::to_string(argument_list.size()));

    if (argc - 1 > (int) (argument_list.size() + opt_arguments.size()))
      error("Too many arguments!");

    int i = 0;
    std::unordered_map<std::string, ArgumentResult> results; 
    for (int j = 1; j < argc; j++) {
      char *string_arg = argv[j];
      if (string_arg[0] == '-' && string_arg[1] == '-') {
        auto search = opt_arguments.find(string_arg + 2);
        if(search == opt_arguments.end()) {
          error(std::string("Could not parse argument: ") + string_arg);
        }
        ArgumentDefinition arg = search->second;
        ArgumentResult parse_result = arg.parser(string_arg);
        results[arg.name] = parse_result;
        // TODO: Deal with arguments that may follow optional args

      } else {
        ArgumentDefinition arg = argument_list[i++];
        ArgumentResult parse_result = arg.parser(string_arg);
        if (!parse_result.parsed_correctly) {
          error(parse_result.err);
        }
        results[arg.name] = parse_result;
      }
    }
    return results;
  }
};
